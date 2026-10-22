#include <QApplication>
#include <QLabel>

int main(int argc, char *argv[])
{
    QApplication app(argc, argv);

    QLabel window("vulture-ui");
    window.setWindowTitle("vulture");
    window.setAlignment(Qt::AlignCenter);
    window.resize(480, 320);
    window.show();

    return app.exec();
}
