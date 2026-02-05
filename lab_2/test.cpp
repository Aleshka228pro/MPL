#include <iostream>
using namespace std;

int square(int x) {
    return x * x;
}

int maxValue(int a, int b) {
    if (a > b) {
        return a;
    } else {
        return b;
    }
}

int main() {
    int n;
    int a, b;

    cout << "Enter n:" << endl;
    cin >> n;

    cout << "Enter a and b:" << endl;
    cin >> a >> b;

    cout << "Max value: " << maxValue(a, b) << endl;

    for (int i = 0; i < n; i++) {
        cout << square(i) << endl;
    }

    return 0;
}