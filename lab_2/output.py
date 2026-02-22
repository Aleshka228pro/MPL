def square(x):
    return x * x
def max_value(a, b):
    if a > b:
        return a
    else:
        return b
if __name__ == '__main__':
    n = None
    a = None
    b = None
    print("Введите n: ")
    n = int(input())
    print("Введите a и b: ")
    a, b = map(int, input().split())
    print("Максимальное значение: ", max_value(a, b))
    for i in range(0, n):
        print(square(i))