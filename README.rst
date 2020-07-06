AsyncAkumuli
============

Welcome to `AsyncAkumuli <https://github.com/M-o-a-T/asyncakumuli>`__!

This library contains a small async wrapper for submission of data to,
and (rudimentary) reading from, `Akumuli <https://akumuli.org>`__.

Also included:

* A basic unit test
* Test scaffolding, to easily test your own code
* a utility to read the last timestamp, as Akumuli doesn't go backwards
* helper code to interpred RRD / collectd input so that you can feed those
  to Akumuli
* a couple of example scripts

Tests and collectd support currently require Trio because (a) anyio doesn't
support subprocesses, (b) UDP socket support in anyio is kindof limited.

