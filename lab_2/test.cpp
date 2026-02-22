#include <iostream>
using namespace std;

int square(int x) {
    return x * x;
}

int max_value(int a, int b) {
    if (a > b) {
        return a;
    } else {
        return b;
    }
}

int main() {
    int n;
    int a, b;

    cout << "Введите n: " << endl;
    cin >> n;

    cout << "Введите a и b: " << endl;
    cin >> a >> b;

    cout << "Максимальное значение: " << max_value(a, b) << endl;

    for (int i = 0; i < n; i++) {
        cout << square(i) << endl;
    }

    return 0;
}