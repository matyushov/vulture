// SPDX-License-Identifier: GPL-3.0-or-later
#include "util/Backoff.h"
#include "util/Format.h"
#include "util/RingBuffer.h"
#include "util/SemanticColors.h"

#include <QtTest>

#include <cmath>

using namespace vulture;

namespace {
double luminance(const QColor &c)
{
    auto lin = [](qreal v) { return v <= 0.03928 ? v / 12.92 : std::pow((v + 0.055) / 1.055, 2.4); };
    return 0.2126 * lin(c.redF()) + 0.7152 * lin(c.greenF()) + 0.0722 * lin(c.blueF());
}
double contrast(const QColor &a, const QColor &b)
{
    const double la = luminance(a), lb = luminance(b);
    return (std::max(la, lb) + 0.05) / (std::min(la, lb) + 0.05);
}
QPalette makePalette(const QColor &window, const QColor &base, const QColor &text)
{
    QPalette p;
    p.setColor(QPalette::Window, window);
    p.setColor(QPalette::Base, base);
    p.setColor(QPalette::Text, text);
    p.setColor(QPalette::WindowText, text);
    return p;
}
} // namespace

class TstUtil : public QObject
{
    Q_OBJECT
private slots:
    void ringBufferOrderAndEviction()
    {
        RingBuffer<int> r(3);
        QVERIFY(r.empty());
        QVERIFY(!r.push(1));
        QVERIFY(!r.push(2));
        QVERIFY(!r.push(3));
        QVERIFY(r.full());
        QVERIFY(r.push(4)); // evicts 1
        QCOMPARE(r.size(), std::size_t(3));
        QCOMPARE(r.at(0), 2);
        QCOMPARE(r.at(2), 4);
        QCOMPARE(r.newest(), 4);
        QCOMPARE(r.newest(2), 2);
        r.popOldest();
        QCOMPARE(r.size(), std::size_t(2));
        QCOMPARE(r.oldest(), 3);
        r.clear();
        QVERIFY(r.empty());
        r.push(9);
        QCOMPARE(r.newest(), 9);
    }

    void ringBufferMemoryIsBounded()
    {
        RingBuffer<int> r(50'000);
        for (int i = 0; i < 120'000; ++i)
            r.push(i);
        QCOMPARE(r.size(), std::size_t(50'000));
        QCOMPARE(r.oldest(), 70'000);
        QCOMPARE(r.newest(), 119'999);
    }

    void compactUsd_data()
    {
        QTest::addColumn<double>("value");
        QTest::addColumn<QString>("text");
        QTest::newRow("millions") << 1'234'567.0 << "$1.23M";
        QTest::newRow("thousands") << 45'600.0 << "$45.6K";
        QTest::newRow("hundreds of thousands") << 123'456.0 << "$123K";
        QTest::newRow("small") << 789.0 << "$789";
        QTest::newRow("cents") << 0.5 << "$0.50";
        QTest::newRow("billions") << 2.5e9 << "$2.50B";
        QTest::newRow("negative") << -1'500'000.0 << "-$1.50M";
        QTest::newRow("rounds up to next unit") << 999'999.0 << "$1.00M";
        QTest::newRow("nan") << std::nan("") << "-";
    }
    void compactUsd()
    {
        QFETCH(double, value);
        QFETCH(QString, text);
        QCOMPARE(fmt::compactUsd(value), text);
    }

    void fullAndPrice()
    {
        QCOMPARE(fmt::fullUsd(1234567.891), QStringLiteral("$1,234,567.89"));
        QCOMPARE(fmt::fullUsd(-5), QStringLiteral("-$5.00"));
        QCOMPARE(fmt::price(67890.123), QStringLiteral("67890.12"));
        QCOMPARE(fmt::price(3.14159), QStringLiteral("3.1416"));
        QCOMPARE(fmt::price(0.5), QStringLiteral("0.50000"));
        QCOMPARE(fmt::price(0.00001234), QStringLiteral("0.00001234"));
        QCOMPARE(fmt::quantity(1.5), QStringLiteral("1.5"));
        QCOMPARE(fmt::quantity(1234.0), QStringLiteral("1234"));
        QCOMPARE(fmt::quantity(0.00012), QStringLiteral("0.00012"));
        QCOMPARE(fmt::price(std::nan("")), QStringLiteral("-"));
    }

    void timeAndDuration()
    {
        QCOMPARE(fmt::time(0, true), QStringLiteral("00:00:00.000"));
        QCOMPARE(fmt::isoUtc(1'700'000'000'123LL), QStringLiteral("2023-11-14T22:13:20.123Z"));
        QVERIFY(fmt::dateTime(0, true).endsWith(QStringLiteral("UTC")));
        QCOMPARE(fmt::duration(850), QStringLiteral("850 ms"));
        QCOMPARE(fmt::duration(3200), QStringLiteral("3.2 s"));
        QCOMPARE(fmt::duration(300'000), QStringLiteral("5 min"));
    }

    void exchangeNamesAndOrder()
    {
        QCOMPARE(fmt::exchangeDisplay("okx"), QStringLiteral("OKX"));
        QCOMPARE(fmt::exchangeDisplay("binance"), QStringLiteral("Binance"));
        QCOMPARE(fmt::exchangeDisplay("kraken"), QStringLiteral("Kraken"));
        QStringList names = {"okx", "kraken", "bybit", "binance", "deribit"};
        std::sort(names.begin(), names.end(),
                  [](const QString &a, const QString &b) { return fmt::exchangeOrderKey(a) < fmt::exchangeOrderKey(b); });
        QCOMPARE(names, (QStringList{"binance", "bybit", "okx", "deribit", "kraken"}));
    }

    void backoffGrowsAndIsCapped()
    {
        BackoffConfig cfg;
        cfg.initialMs = 500;
        cfg.maxMs = 8000;
        Backoff b(cfg, [] { return 0; }, [] { return 0.5; }); // jitter factor exactly 1
        QCOMPARE(b.nextDelayMs(), 500);
        QCOMPARE(b.nextDelayMs(), 1000);
        QCOMPARE(b.nextDelayMs(), 2000);
        QCOMPARE(b.nextDelayMs(), 4000);
        QCOMPARE(b.nextDelayMs(), 8000);
        QCOMPARE(b.nextDelayMs(), 8000);
        QCOMPARE(b.attempt(), 6);
    }

    void backoffJitterBounds()
    {
        BackoffConfig cfg;
        cfg.initialMs = 1000;
        cfg.jitter = 0.25;
        Backoff low(cfg, [] { return 0; }, [] { return 0.0; });
        Backoff high(cfg, [] { return 0; }, [] { return 1.0; });
        QCOMPARE(low.nextDelayMs(), 750);
        QCOMPARE(high.nextDelayMs(), 1250);
    }

    void backoffResetsAfterStableConnection()
    {
        qint64 now = 0;
        BackoffConfig cfg;
        cfg.stableMs = 30'000;
        Backoff b(cfg, [&] { return now; }, [] { return 0.5; });
        b.nextDelayMs();
        b.nextDelayMs();
        QCOMPARE(b.attempt(), 2);
        // A flapping connection (up 5 s) keeps the counter.
        b.onConnected();
        now += 5'000;
        b.onDisconnected();
        QCOMPARE(b.attempt(), 2);
        QCOMPARE(b.nextDelayMs(), 2000);
        // A stable connection (up 60 s) resets it.
        b.onConnected();
        now += 60'000;
        b.onDisconnected();
        QCOMPARE(b.attempt(), 0);
        QCOMPARE(b.nextDelayMs(), 500);
    }

    void semanticColorsReadableInBothThemes_data()
    {
        QTest::addColumn<QPalette>("palette");
        QTest::addColumn<bool>("dark");
        QTest::newRow("light") << makePalette(QColor("#ececec"), QColor("#ffffff"), QColor("#1a1a1a")) << false;
        QTest::newRow("dark") << makePalette(QColor("#1e1e1e"), QColor("#2a2a2a"), QColor("#e8e8e8")) << true;
    }
    void semanticColorsReadableInBothThemes()
    {
        QFETCH(QPalette, palette);
        QFETCH(bool, dark);
        const SemanticColors c = semanticColors(palette);
        QCOMPARE(c.dark, dark);
        const QColor base = palette.color(QPalette::Base);
        for (const QColor &fg : {c.longText, c.shortText, c.warningText})
            QVERIFY2(contrast(fg, base) >= 3.0, qPrintable(fg.name() + " on " + base.name()));
        // Text stays readable on the tinted row backgrounds as well.
        const QColor text = palette.color(QPalette::Text);
        for (const QColor &bg : {c.longTint, c.shortTint, c.warningTint, c.largeTint})
            QVERIFY2(contrast(text, bg) >= 4.5, qPrintable(text.name() + " on " + bg.name()));
        // Long and short must differ in more than lightness.
        QVERIFY(qAbs(c.longText.hueF() - c.shortText.hueF()) > 0.2);
    }
};

QTEST_MAIN(TstUtil)
#include "tst_Util.moc"
