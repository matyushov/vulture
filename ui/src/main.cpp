// SPDX-License-Identifier: GPL-3.0-or-later
#include "app/Settings.h"
#include "ui/MainWindow.h"

#include <QApplication>
#include <QCommandLineParser>
#include <QTimer>

int main(int argc, char *argv[])
{
    QApplication app(argc, argv);
    QApplication::setApplicationName(QStringLiteral("vulture-ui"));
    QApplication::setOrganizationName(QStringLiteral("vulture"));
    QApplication::setApplicationVersion(QStringLiteral(VULTURE_UI_VERSION));

    QCommandLineParser parser;
    parser.setApplicationDescription(QStringLiteral("vulture: liquidation and open-interest client"));
    parser.addHelpOption();
    parser.addVersionOption();
    QCommandLineOption settingsOption(QStringLiteral("settings-file"),
                                      QStringLiteral("Use this settings (INI) file instead of the default one."),
                                      QStringLiteral("path"));
    parser.addOption(settingsOption);
    parser.process(app);

    vulture::Settings settings(parser.value(settingsOption));
    vulture::MainWindow window(&settings);
    window.show();
    // Show the connection dialog once the window is up (prefilled from the saved settings).
    QTimer::singleShot(0, &window, [&window] { window.promptConnect(); });
    return app.exec();
}
