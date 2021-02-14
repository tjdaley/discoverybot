from util.database import Database

def main():
    db = Database()
    db.connect()
    d = db.get_client_id('Denton', '2009-50481-367', 'tdaley@koonsfuller.com')
    print(d)


if __name__ == '__main__':
    main()