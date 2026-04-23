import sys
from sqldocs import generate_doc


def main():
    input_file = sys.argv[1]
    html = generate_doc(input_file)
    print(html)


if __name__ == "__main__":
    main()