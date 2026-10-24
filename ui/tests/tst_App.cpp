// SPDX-License-Identifier: GPL-3.0-or-later
#include "TestUtil.h"
#include "app/LargeLiquidationNotifier.h"
#include "app/Settings.h"
#include "model/Favorites.h"
#include "net/ConnectionConfig.h"
#include "net/ErrorText.h"

#include <grpcpp/support/status_code_enum.h>

#include <QFile>
#include <QSignalSpy>
#include <QTemporaryDir>
#include <QtTest>

using namespace vulture;
using namespace vulture::testing;

class TstApp : public QObject
{
    Q_OBJECT

    static QString read(const QString &path)
    {
        QFile f(path);
        return f.open(QIODevice::ReadOnly) ? QString::fromUtf8(f.readAll()) : QString();
    }

private slots:
    // Qt reports the owner bits twice (Owner and User flags).
    static bool isMode0600(const QString &path)
    {
        const QFileDevice::Permissions p = QFile::permissions(path);
        constexpr QFileDevice::Permissions ownerRw = QFileDevice::ReadOwner | QFileDevice::WriteOwner;
        constexpr QFileDevice::Permissions rest = QFileDevice::ExeOwner | QFileDevice::ReadGroup | QFileDevice::WriteGroup |
                                                  QFileDevice::ExeGroup | QFileDevice::ReadOther | QFileDevice::WriteOther |
                                                  QFileDevice::ExeOther;
        return (p & ownerRw) == ownerRw && !(p & rest);
    }

    // --- Settings ---
    void settingsDefaults()
    {
        QTemporaryDir dir;
        Settings s(dir.filePath("settings.ini"));
        QVERIFY(s.utc());
        QCOMPARE(s.largeThresholdUsd(), 100'000.0);
        QVERIFY(s.notificationsEnabled());
        QVERIFY(!s.rememberToken());
        QVERIFY(s.connection().tls);
        QVERIFY(s.favorites().isEmpty());
    }

    void settingsRoundTrip()
    {
        QTemporaryDir dir;
        const QString path = dir.filePath("nested/settings.ini");
        {
            Settings s(path);
            ConnectionConfig c;
            c.address = "example.invalid:50051";
            c.token = "placeholder-token";
            c.tls = true;
            c.caFile = "/path/to/ca.pem";
            s.setConnection(c, true);
            s.setUtc(false);
            s.setLargeThresholdUsd(250'000);
            s.setNotificationsEnabled(false);
            s.setFavorites({"binance/BTC-USDT-PERP", "okx/ETH-USDT-PERP"});
            s.setWindowGeometry(QByteArray("geom"));
            s.setWindowState(QByteArray("state"));
            s.setDockLayout("binance", QByteArray("\x01\x02layout", 8));
        }
        Settings s(path);
        const ConnectionConfig c = s.connection();
        QCOMPARE(c.address, QStringLiteral("example.invalid:50051"));
        QCOMPARE(c.token, QStringLiteral("placeholder-token"));
        QCOMPARE(c.caFile, QStringLiteral("/path/to/ca.pem"));
        QVERIFY(s.rememberToken());
        QVERIFY(!s.utc());
        QCOMPARE(s.largeThresholdUsd(), 250'000.0);
        QVERIFY(!s.notificationsEnabled());
        QCOMPARE(s.favorites(), (QStringList{"binance/BTC-USDT-PERP", "okx/ETH-USDT-PERP"}));
        QCOMPARE(s.windowGeometry(), QByteArray("geom"));
        QCOMPARE(s.windowState(), QByteArray("state"));
        QCOMPARE(s.dockLayout("binance"), QByteArray("\x01\x02layout", 8));
        QVERIFY(s.dockLayout("bybit").isEmpty());
        s.clearDockLayouts();
        QVERIFY(s.dockLayout("binance").isEmpty());
    }

    void tokenIsNotStoredUnlessRemembered()
    {
        QTemporaryDir dir;
        const QString path = dir.filePath("settings.ini");
        Settings s(path);
        ConnectionConfig c;
        c.address = "example.invalid:50051";
        c.token = "super-secret-placeholder";
        s.setConnection(c, true);
        QVERIFY(read(path).contains("super-secret-placeholder"));
        s.setConnection(c, false); // un-ticking "remember" removes it again
        QVERIFY(!read(path).contains("super-secret-placeholder"));
        QVERIFY(Settings(path).connection().token.isEmpty());
    }

    void favoritesPersistThroughSettings()
    {
        QTemporaryDir dir;
        const QString path = dir.filePath("settings.ini");
        {
            Settings s(path);
            Favorites fav;
            fav.setFavorite("binance", "BTC-USDT-PERP", true);
            fav.setFavorite("bybit", "ETH-USDT-PERP", true);
            s.setFavorites(fav.toList());
        }
        Settings s(path);
        Favorites fav;
        fav.fromList(s.favorites());
        QVERIFY(fav.isFavorite("binance", "BTC-USDT-PERP"));
        QVERIFY(fav.isFavorite("bybit", "ETH-USDT-PERP"));
        QVERIFY(!fav.isFavorite("okx", "ETH-USDT-PERP"));
    }

    void settingsFileIsPrivate()
    {
#ifdef Q_OS_WIN
        QSKIP("POSIX file modes are not supported on Windows");
#else
        QTemporaryDir dir;
        const QString path = dir.filePath("settings.ini");
        Settings s(path);
        QVERIFY2(s.fileIsPrivate(), "a new file must be created private");
        QVERIFY(isMode0600(path));
        ConnectionConfig c;
        c.address = "example.invalid:1";
        c.token = "placeholder-token";
        for (int i = 0; i < 3; ++i) {
            s.setConnection(c, true); // QSettings replaces the file on every write
            s.setUtc(i % 2 == 0);
            QVERIFY(isMode0600(path));
        }
#endif
    }

    void existingLooseFileIsTightened()
    {
#ifdef Q_OS_WIN
        QSKIP("POSIX file modes are not supported on Windows");
#else
        QTemporaryDir dir;
        const QString path = dir.filePath("settings.ini");
        {
            QFile f(path);
            QVERIFY(f.open(QIODevice::WriteOnly));
            f.write("[connection]\naddress=a:1\n");
            f.close();
            QVERIFY(f.setPermissions(QFileDevice::ReadOwner | QFileDevice::WriteOwner | QFileDevice::ReadGroup |
                                     QFileDevice::ReadOther));
        }
        Settings s(path);
        QVERIFY(s.fileIsPrivate());
        QCOMPARE(s.connection().address, QStringLiteral("a:1"));
#endif
    }

    // --- Connection config ---
    void addressParsing_data()
    {
        QTest::addColumn<QString>("address");
        QTest::addColumn<bool>("ok");
        QTest::addColumn<QString>("host");
        QTest::addColumn<int>("port");
        QTest::newRow("host") << "example.invalid:50051" << true << "example.invalid" << 50051;
        QTest::newRow("spaces") << "  127.0.0.1:1234 " << true << "127.0.0.1" << 1234;
        QTest::newRow("ipv6") << "[::1]:50051" << true << "::1" << 50051;
        QTest::newRow("no port") << "example.invalid" << false << "" << 0;
        QTest::newRow("bad port") << "example.invalid:99999" << false << "" << 0;
        QTest::newRow("scheme") << "https://example.invalid:443" << false << "" << 0;
        QTest::newRow("empty") << "" << false << "" << 0;
        QTest::newRow("bare ipv6") << "::1:50051" << false << "" << 0;
        QTest::newRow("path") << "example.invalid/x:1" << false << "" << 0;
    }
    void addressParsing()
    {
        QFETCH(QString, address);
        QFETCH(bool, ok);
        QFETCH(QString, host);
        QFETCH(int, port);
        QString error;
        const auto hp = parseAddress(address, &error);
        QCOMPARE(hp.has_value(), ok);
        if (ok) {
            QCOMPARE(hp->host, host);
            QCOMPARE(hp->port, port);
        } else {
            QVERIFY(!error.isEmpty());
        }
    }

    void plaintextOnlyForLoopback()
    {
        QVERIFY(isLoopbackHost("localhost"));
        QVERIFY(isLoopbackHost("127.0.0.1"));
        QVERIFY(isLoopbackHost("127.1.2.3"));
        QVERIFY(isLoopbackHost("::1"));
        QVERIFY(!isLoopbackHost("example.invalid"));
        QVERIFY(!isLoopbackHost("10.0.0.5"));
        QVERIFY(!isLoopbackHost("0.0.0.0"));

        ConnectionConfig c;
        c.address = "10.0.0.5:50051";
        c.token = "t";
        c.tls = false;
        QVERIFY(validate(c).contains("Plaintext"));
        c.tls = true;
        QVERIFY(validate(c).isEmpty());
        c.address = "127.0.0.1:50051";
        c.tls = false;
        QVERIFY(validate(c).isEmpty());
        c.token = "  ";
        QVERIFY(validate(c).contains("token"));
    }

    // --- Error texts ---
    void grpcErrorsAreReadable()
    {
        using grpc::StatusCode;
        QVERIFY(describeGrpcError(StatusCode::UNAUTHENTICATED, "", true).contains("token"));
        QVERIFY(describeGrpcError(StatusCode::RESOURCE_EXHAUSTED, "", true).contains("RESOURCE_EXHAUSTED"));
        QVERIFY(describeGrpcError(StatusCode::OUT_OF_RANGE, "", true).contains("older"));
        QVERIFY(describeGrpcError(StatusCode::UNAVAILABLE, "Ssl handshake failed: CERTIFICATE_VERIFY_FAILED", true).contains("TLS"));
        QVERIFY(describeGrpcError(StatusCode::UNAVAILABLE, "failed to connect to all addresses", true).contains("unreachable"));
        QVERIFY(describeGrpcError(StatusCode::UNAVAILABLE, "DNS resolution failed", true).contains("resolved"));
        QVERIFY(describeGrpcError(StatusCode::DEADLINE_EXCEEDED, "", true).contains("timed out"));
        QCOMPARE(reactionFor(StatusCode::UNAUTHENTICATED), Reaction::Fatal);
        QCOMPARE(reactionFor(StatusCode::OUT_OF_RANGE), Reaction::ResubscribeLive);
        QCOMPARE(reactionFor(StatusCode::RESOURCE_EXHAUSTED), Reaction::Retry);
        QCOMPARE(reactionFor(StatusCode::UNAVAILABLE), Reaction::Retry);
        QCOMPARE(reactionFor(StatusCode::CANCELLED), Reaction::Retry);
    }

    // --- Notification rate limit ---
    void notifierLimitsToOnePerTenSecondsAndMerges()
    {
        qint64 now = 1000;
        LargeLiquidationNotifier n([&] { return now; });
        QSignalSpy spy(&n, &LargeLiquidationNotifier::notify);
        n.submit(liq("binance", "BTC-USDT-PERP", LiqSide::Long, 250'000));
        QCOMPARE(spy.count(), 1); // the first shows immediately
        QVERIFY(spy.at(0).at(1).toString().contains("Binance BTC-USDT-PERP long liquidation $250K"));

        now += 2000;
        n.submit(liq("okx", "ETH-USDT-PERP", LiqSide::Short, 120'000));
        now += 3000;
        n.submit(liq("bybit", "SOL-USDT-PERP", LiqSide::Long, 900'000));
        n.tick();
        QCOMPARE(spy.count(), 1); // still inside the window

        now += 5000; // 10 s since the first message
        n.tick();
        QCOMPARE(spy.count(), 2);
        QCOMPARE(spy.at(1).at(0).toString(), QStringLiteral("2 large liquidations"));
        const QString body = spy.at(1).at(1).toString();
        QVERIFY2(body.contains("Total $1.02M") && body.contains("Largest: Bybit SOL-USDT-PERP"), qPrintable(body));

        n.tick();
        QCOMPARE(spy.count(), 2); // nothing pending, nothing shown
    }

    void notifierCanBeDisabled()
    {
        qint64 now = 0;
        LargeLiquidationNotifier n([&] { return now; });
        QSignalSpy spy(&n, &LargeLiquidationNotifier::notify);
        n.setEnabled(false);
        n.submit(liq("binance", "BTC-USDT-PERP", LiqSide::Long, 250'000));
        now += 60'000;
        n.tick();
        QCOMPARE(spy.count(), 0);
        n.setEnabled(true);
        n.submit(liq("binance", "BTC-USDT-PERP", LiqSide::Long, 250'000));
        QCOMPARE(spy.count(), 1);
    }
};

QTEST_MAIN(TstApp)
#include "tst_App.moc"
