pg_timelord
===========

Features
--------

A timelord to let you go back in time!

**WARNING** : This extension is a proof of concept, and not intended for
production use.

Installation
============

Compiling
---------

The module can be built using the standard PGXS infrastructure. For this to
work, the ``pg_config`` program must be available in your $PATH. Instruction to
install follows:

    # git clone or download and extract a tarball
    cd pg_timelord
    make
    make install

Configuration
-------------

To be able to use this extension, you need to:

    * add pg_timelord in `shared_preload_librarires`
    * enable `track_commit_timestamp`
    * restart postgres

Usage
=====

The module is now available.  If you want to travel back in time, you just need
to set your destination, as long as you have tracked commit timestamps
available.  For instance:

    rjuju=# set pg_timelord.ts = '2016-10-31 23:59:59 GMT';

And now, just run your queries.

License
=======

pg_timelord is free software distributed under the PostgreSQL license.

Copyright (c) 2016-2023, Julien Rouhaud.

