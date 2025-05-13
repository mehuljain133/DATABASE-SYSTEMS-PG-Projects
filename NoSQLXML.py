# Unit-VII Introduction to NoSQL databases, XML databases.

# Importing necessary libraries
import pymongo
import sqlite3
from lxml import etree
from bson import ObjectId

# ------------------------------
# NoSQL Database - MongoDB Example
# ------------------------------

# Connect to MongoDB server and database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['test_db']
collection = db['students']

# Insert documents into MongoDB (NoSQL)
def insert_student_nosql(name, age, dept):
    student = {
        "name": name,
        "age": age,
        "department": dept
    }
    result = collection.insert_one(student)
    print(f"Inserted student with ID: {result.inserted_id}")

# Query from MongoDB (NoSQL)
def query_students_nosql():
    students = collection.find()
    print("\nStudents in MongoDB:")
    for student in students:
        print(student)

# ------------------------------
# XML Database Example
# ------------------------------

# Create or connect to an SQLite database for storing XML data
conn = sqlite3.connect('xml_database.db')
cursor = conn.cursor()

# Create table to store XML data
def create_xml_table():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS xml_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            xml_content TEXT
        )
    """)
    conn.commit()

# Insert XML data into SQLite
def insert_xml_data(xml_string):
    cursor.execute("INSERT INTO xml_data (xml_content) VALUES (?)", (xml_string,))
    conn.commit()

# Query XML data from SQLite
def query_xml_data():
    cursor.execute("SELECT * FROM xml_data")
    rows = cursor.fetchall()
    print("\nXML Data in SQLite:")
    for row in rows:
        print(row[1])  # Printing the XML content (stored as text)

# ------------------------------
# XML Parsing and Querying (using lxml)
# ------------------------------

# Parse an XML string and find elements
def parse_and_query_xml(xml_string):
    tree = etree.XML(xml_string)
    students = tree.xpath('//student')
    print("\nParsed XML Students:")
    for student in students:
        name = student.xpath('./name/text()')
        dept = student.xpath('./department/text()')
        print(f"Name: {name[0]}, Department: {dept[0]}")

# ------------------------------
# Example XML Data (XML Database)
# ------------------------------

example_xml = """
<school>
    <student>
        <name>Alice</name>
        <age>20</age>
        <department>Computer Science</department>
    </student>
    <student>
        <name>Bob</name>
        <age>22</age>
        <department>Electrical Engineering</department>
    </student>
    <student>
        <name>Charlie</name>
        <age>21</age>
        <department>Physics</department>
    </student>
</school>
"""

# ------------------------------
# Running the Code
# ------------------------------
if __name__ == "__main__":
    # ------------------------------
    # NoSQL (MongoDB) Example
    # ------------------------------

    print("Inserting data into MongoDB (NoSQL)...")
    insert_student_nosql("Alice", 20, "Computer Science")
    insert_student_nosql("Bob", 22, "Electrical Engineering")
    insert_student_nosql("Charlie", 21, "Physics")
    query_students_nosql()

    # ------------------------------
    # XML Database Example (SQLite)
    # ------------------------------
    create_xml_table()
    print("\nInserting XML data into SQLite...")
    insert_xml_data(example_xml)
    query_xml_data()

    # ------------------------------
    # XML Parsing with lxml
    # ------------------------------
    parse_and_query_xml(example_xml)

