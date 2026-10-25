# QCustomPlot (vendored)

* Upstream: QCustomPlot 2.1.1 (2022-11-06), https://www.qcustomplot.com/ , file `QCustomPlot-source.tar.gz`
  (sha256 `5e2d22dec779db8f01f357cbdb25e54fbcf971adaee75eae8d7ad2444487182f`), only `qcustomplot.h` and `qcustomplot.cpp`.
* License: GPL-3.0-or-later (`LICENSE.txt`), the same as vulture-ui. Copyright (C) 2011-2022 Emanuel Eichhammer.
* Local change (one place): in `qcustomplot.h` the include of `<QtPrintSupport/QtPrintSupport>` is skipped when
  `QT_NO_PRINTER` is defined. The `vulture-qcustomplot` CMake target defines `QT_NO_PRINTER` publicly, so the UI does not
  need the PrintSupport module (only `savePdf` is lost, which vulture does not use).
* Warnings of this directory are silenced in `ui/CMakeLists.txt` (`vulture-qcustomplot` target only).
* To update: replace the two files from a newer release, re-apply the include patch, adjust this note.
