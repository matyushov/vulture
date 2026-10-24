// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

// Throwaway self-signed certificate for CN=localhost (SAN: localhost, 127.0.0.1), valid for 100 years,
// generated only for the loopback TLS tests. It protects nothing: never use it anywhere else.
namespace vulture::testcerts {

inline constexpr const char *kCertPem = R"PEM(-----BEGIN CERTIFICATE-----
MIIBezCCASCgAwIBAgIUNJtVkPYNWECrOMS/imjXPJnPpUEwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MTAyNDIxMTY1NloYDzIxMjYwOTMw
MjExNjU2WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAARuQmiNF8EzgGtYEPTAacQHARDLYxQ8s36PPG0Yu4xpO3Hahx2ByiCq
1tCRGhtkoKFMcslihoFHpSptl4BSfqPCo04wTDAdBgNVHQ4EFgQUsVdYvzQ2yRoe
2yHR3iQfu91IOyMwDwYDVR0TAQH/BAUwAwEB/zAaBgNVHREEEzARgglsb2NhbGhv
c3SHBH8AAAEwCgYIKoZIzj0EAwIDSQAwRgIhAJ4q5OkDKf2menxf9eqrj/Cqmqhb
zoOUy3ajJBTzYYUtAiEAtuPW/FWZ+h/5YPwgOjv2PU02tzdpiAkADG1MyVGhLBQ=
-----END CERTIFICATE-----
)PEM";

inline constexpr const char *kKeyPem = R"PEM(-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgTyN820y+xdXZQaCV
yaHAn5BW8bqwcD3/dF9GUe7O18ahRANCAARuQmiNF8EzgGtYEPTAacQHARDLYxQ8
s36PPG0Yu4xpO3Hahx2ByiCq1tCRGhtkoKFMcslihoFHpSptl4BSfqPC
-----END PRIVATE KEY-----
)PEM";

} // namespace vulture::testcerts
