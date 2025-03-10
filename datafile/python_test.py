import pandas as pd


def load_data():
    df = pd.read_csv("./data.csv", encoding='ISO-8859-1')
    print(df.head())
    print(df.shape)
    print(df.columns)
    df.drop("InvoiceNo", axis=1, inplace=True)
    df.drop("Description", axis=1, inplace=True)
    print(df.columns)
    print(df.shape)
    df.drop()

def main():
    load_data()


if __name__ == "__main__":
    main()