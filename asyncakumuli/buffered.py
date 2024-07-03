_DEFAULT_LIMIT = 4096

from anyio import EndOfStream


class IncompleteReadError(RuntimeError):
    def __init__(self, chunk):
        super().__init__()
        self.chunk = chunk


class LimitOverrunError(RuntimeError):
    def __init__(self, txt, offset):
        super().__init__(txt)
        self.offset = offset


class AbstractStreamModifier:  # pylint: disable=abstract-method
    """Interpose on top of a stream.

    Use this class as a base for writing your own stream modifiers.
    """

    def __init__(self, *, lower_stream=None):
        self._lower_stream = lower_stream

    # AsyncResource

    async def __aenter__(self):
        await self._lower_stream.__aenter__()
        return self

    async def __aexit__(self, *err):
        return await self._lower_stream.__aexit__(*err)

    async def aclose(self):
        await self._lower_stream.aclose()

    # SendStream

    async def send(self, data):
        await self._lower_stream.send(data)

    async def send_eof(self):
        await self._lower_stream.send_eof()


class BufferedReader(AbstractStreamModifier):
    """A stream modifier that buffers read data."""

    def __init__(
        self,
        lower_stream=None,
        data=None,
        read_limit=_DEFAULT_LIMIT // 2,
        write_limit=0,  # pylint:disable=unused-argument
    ):
        if (lower_stream is None) == (data is None):
            raise RuntimeError("provide either lower_stream or data")
        if lower_stream is not None:
            lower_stream = aiter(lower_stream)
        super().__init__(lower_stream=lower_stream)
        self._read_limit = read_limit
        if data is None:
            self._read_buffer = bytearray()
        else:
            self._read_buffer = data

        if read_limit <= 0:
            raise ValueError("Limit cannot be <= 0")

    async def _rd(self, n):
        ls = self._lower_stream
        if hasattr(ls,"receive"):
            return await ls.receive(n)
        if hasattr(ls,"read_size"):
            ls.read_size = n
        return await ls.__anext__()

    @property
    def read_buffer(self):
        return self._read_buffer

    @read_buffer.setter
    def read_buffer(self, buf):
        self._read_buffer = buf

    @property
    def _buffer(self):
        return self._read_buffer

    def __repr__(self):
        info = [self.__class__.__name__]
        info.append(f"wraps={self._lower_stream}")
        if self._read_limit != _DEFAULT_LIMIT:
            info.append(f"limit={self._read_limit}")
        return "<{}>".format(" ".join(info))

    async def receive(self, n=None):  # pylint: disable=arguments-differ
        """Get at most n bytes from the buffer.

        If the buffer is empty, fill it.
        """
        buf = self._read_buffer
        if not buf:
            if self._lower_stream is None:
                return None
            data = await self._rd(n or self._read_limit)
            if not data:
                return data
            if n is None or len(data) <= n:
                return data
            buf[:] = data[n:]
            data = data[:n]
        elif n is None or len(buf) == n:
            data = bytes(buf)
            buf.clear()
        else:
            data = bytes(buf[:n])
            del buf[:n]
        return data

    async def extend_buffer(self):
        """Extends the buffer with more data.

        This method returns the number of new bytes.
        If zero, EOF has been seen.
        """
        if self._lower_stream is None:
            return False
        try:
            data = await self._rd(len(self._read_buffer) + 512)
        except EndOfStream:
            return False
        self._read_buffer.extend(data)
        return len(data)

    async def readline(self):
        """Read chunk of data from the stream until newline (b'\n') is found.

        On success, return chunk that ends with newline. If only partial
        line can be read due to EOF, return incomplete line without
        terminating newline. When EOF was reached while no bytes read, empty
        bytes object is returned.

        If limit is reached, ValueError will be raised. In that case, if
        newline was found, complete line including newline will be removed
        from internal buffer. Else, internal buffer will be cleared. Limit is
        compared against part of the line without newline.
        """
        sep = b"\n"
        seplen = len(sep)
        buf = self.read_buffer
        try:
            line = await self.readuntil(sep)
        except IncompleteReadError as e:
            return e.chunk
        except LimitOverrunError as e:
            if buf.startswith(sep, e.offset):
                del buf[: e.offset + seplen]
            else:
                buf.clear()
            raise ValueError(e.args[0]) from e
        return line

    async def readuntil(self, separator=b"\n"):
        """Read data from the stream until ``separator`` is found.

        On success, the data and separator will be removed from the
        internal buffer (consumed). Returned data will include the
        separator at the end.

        Configured stream limit is used to check result. Limit sets the
        maximal length of data that can be returned, not counting the
        separator.

        If an EOF occurs and the complete separator is still not found,
        an IncompleteReadError exception will be raised, and the internal
        buffer will be reset.  The IncompleteReadError.partial attribute
        may contain the separator partially.

        If the data cannot be read because of over limit, a
        LimitOverrunError exception  will be raised, and the data
        will be left in the internal buffer, so it can be read again.
        """
        buf = self.read_buffer
        seplen = len(separator)
        if seplen == 0:
            raise ValueError("Separator should be at least one-byte string")

        # Consume whole buffer except last bytes, which length is
        # one less than seplen. Let's check corner cases with
        # separator='SEPARATOR':
        # * we have received almost complete separator (without last
        #   byte). i.e buffer='some textSEPARATO'. In this case we
        #   can safely consume len(separator) - 1 bytes.
        # * last byte of buffer is first byte of separator, i.e.
        #   buffer='abcdefghijklmnopqrS'. We may safely consume
        #   everything except that last byte, but this require to
        #   analyze bytes of buffer that match partial separator.
        #   This is slow and/or require FSM. For this case our
        #   implementation is not optimal, since require rescanning
        #   of data that is known to not belong to separator. In
        #   real world, separator will not be so long to notice
        #   performance problems. Even when reading MIME-encoded
        #   messages :)

        # `offset` is the number of bytes from the beginning of the buffer
        # where there is no occurrence of `separator`.
        offset = 0

        # Loop until we find `separator` in the buffer, exceed the buffer size,
        # or an EOF has happened.
        eof = False

        while True:
            buflen = len(buf)

            # Check if we now have enough data in the buffer for `separator` to
            # fit.
            if buflen - offset >= seplen:
                isep = buf.find(separator, offset)

                if isep != -1:
                    # `separator` is in the buffer. `isep` will be used later
                    # to retrieve the data.
                    break

                # see upper comment for explanation.
                offset = buflen + 1 - seplen
                if offset > self._read_limit:
                    raise LimitOverrunError(
                        "Separator is not found, and chunk exceed the limit", offset
                    )

            # Complete message (with full separator) may be present in buffer
            # even when EOF flag is set. This may happen when the last chunk
            # adds data which makes separator be found. That's why we check for
            # EOF *ater* inspecting the buffer.
            if eof:
                chunk = bytes(buf)
                try:
                    buf.clear()
                except AttributeError:
                    buf = b""
                raise IncompleteReadError(chunk)

            # _wait_for_data() will resume reading if stream was paused.
            eof = not await self.extend_buffer()

        if isep > self._read_limit:
            raise LimitOverrunError("Separator is found, but chunk is longer than limit", isep)

        chunk = buf[: isep + seplen]
        try:
            del buf[: isep + seplen]
        except TypeError:
            self.read_buffer = buf[isep + seplen :]
            return chunk
        else:
            return bytes(chunk)
