def square(x):
    return x * x
def maxValue(a, b):
    if a > b:
        return a
    else:
        return b
if __name__ == '__main__':
    n = None
    a = None
    b = None
    print("Enter n:")
    n = int(input())
    print("Enter a and b:")
    a, b = map(int, input().split())
    print("Max value: ", maxValue(a, b))
    for i in range(0, n):
        print(square(i))