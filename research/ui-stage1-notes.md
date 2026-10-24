# UI stage 1 notes (2026-10-25)

## S-2 spike: grpc++ in Qt
* conan packages `grpc/1.84.0`, `protobuf/6.33.5` install with `--build=never` for `ui/conan/profiles/macos-armv8` and
  `ui/conan.lock`; nothing was compiled from source. The local compiler is a newer Apple clang than the profile's
  apple-clang 17, and the build works. The profile's deployment target (13.0) gives a linker warning against Qt (14.4).
* grpc++ runs on a plain `std::thread` next to Qt; events reach the GUI thread through a mutex-guarded vector drained
  by a 33 ms timer (no per-event signal). Cancellation: `ClientContext::TryCancel` from the GUI thread.
* TLS roots: the client feeds `QSslConfiguration::systemCaCertificates()` as PEM to `SslCredentials`; a test shows a
  certificate outside the system store is rejected with a TLS error (so the PEM bundle parses), and a private CA file
  works. Not verified against a real public CA server (owner does it).
* `ClientReader::WaitForInitialMetadata()` returns void and does not tell success from a rejected call, so "Live" is
  declared on the first event or after 300 ms without the call finishing.

## Measurements (in-process fake server, QT_QPA_PLATFORM=offscreen, Apple silicon, Release)
* Paced 5 000 events/s, 40 000 events, main window shown: all delivered, 4 986 events/s; GUI 5 ms timer gaps p99 17 ms,
  max 17 ms.
* Unthrottled 200 000 events: 40 800 events/s end to end into the models; gaps p99 17 ms, max 19 ms.
* 3 000-event burst with the window and three exchange tabs: max GUI stall 18 ms (offscreen), 46 ms (cocoa).

## Screenshots (looked at, light and dark)
Offscreen ignores `QStyleHints::setColorScheme`, so the test installs an explicit palette there; the native cocoa
plugin honors it (also run once by hand). Fixes made after looking: gap/marker messages are spanned over the cells
instead of being cut; dock default sizes applied after first show; admin panel shows no CA for plaintext.

## Open questions for the server
* `Stream.Subscribe` response headers: does tonic send them before the first event? (affects "Live" with a quiet stream)
* No way to ask the server for a short replay on first connect other than `resume_from="0"` (full hot window); UI offers none yet.
