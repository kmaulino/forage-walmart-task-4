import sqlite3, csv

class DatabasePopulator:

    def __init__(self, db):
        self.con = sqlite3.connect(db)
        self.cur = self.con.cursor()

    def close(self):
        self.con.close()

    def populate(self, folder):
        with open(folder + "/shipping_data_0.csv") as data0:
            with open(folder + "/shipping_data_1.csv") as data1:
                with open(folder + "/shipping_data_2.csv") as data2:
                    reader0 = csv.reader(data0)
                    reader1 = csv.reader(data1)
                    reader2 = csv.reader(data2)
                    self.insert_shipment1(reader0)
                    self.insert_shipment2(reader1, reader2)

    def insert_shipment1(self, reader0):
        for i, r in enumerate(reader0):
            if i > 0:
                origin = r[0]
                destination = r[1]
                product_name = r[2]
                quantity = r[4]
                self.insert_product(product_name)
                self.insert_shipment(product_name, quantity, origin, destination)
                print("inserted " + product_name + " from shipment 1")

    def insert_product(self, product_name):
        query = """INSERT OR IGNORE INTO product (name) VALUES(?)"""
        self.cur.execute(query, (product_name, ))
        self.con.commit()

    def insert_shipment(self, product_name, quantity, origin, destination):
        query = """SELECT id FROM product WHERE name = ?"""
        self.cur.execute(query, (product_name, ))
        product_id = self.cur.fetchone()[0]
        query = """INSERT OR IGNORE INTO shipment
                (product_id, quantity, origin, destination)
                VALUES(?, ?, ?, ?)"""
        self.cur.execute(query, (product_id, quantity, origin, destination))
        self.con.commit()

    def insert_shipment2(self, reader1, reader2):
        shipments = {}
        for i, r in enumerate(reader2):
            if i > 0:
                shipment_id = r[0]
                origin = r[1]
                destination = r[2]
                products = {}
                shipments[shipment_id] = [origin, destination, products]
        for i, r in enumerate(reader1):
            if i > 0:
                shipment_id = r[0]
                product_name = r[1]
                products = shipments[shipment_id][2]
                if product_name in products:
                    products[product_name] += 1
                else:
                    products[product_name] = 1
        for origin, destination, products in shipments.values():
            for product_name, quantity in products.items():
                self.insert_product(product_name)
                self.insert_shipment(product_name, quantity, origin, destination)
                print("inserted " + product_name + " from shipment 2")
                
if __name__ == '__main__':
    database_populator = DatabasePopulator("shipment_database.db")
    database_populator.populate("./data")
    database_populator.close()
                
                
            
        
