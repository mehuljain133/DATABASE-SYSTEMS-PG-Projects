# Unit-IV Structured Query Language: DDL, DML, Views, Embedded SQL

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Setup SQLAlchemy ORM
Base = declarative_base()
engine = create_engine("sqlite:///sql_unit.db", echo=True)
Session = sessionmaker(bind=engine)
session = Session()

# -----------------------------
# DDL: Data Definition Language
# -----------------------------

# Creating the tables using SQLAlchemy ORM
class Department(Base):
    __tablename__ = 'departments'
    dept_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    # Relationship to Student table
    students = relationship("Student", back_populates="department")

class Student(Base):
    __tablename__ = 'students'
    roll_no = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    dept_id = Column(Integer, ForeignKey('departments.dept_id'))

    department = relationship("Department", back_populates="students")


# Create tables
def initialize_database():
    Base.metadata.create_all(engine)

    # Insert sample data if tables are empty
    if not session.query(Department).first():
        d1 = Department(name='Computer Science')
        d2 = Department(name='Electrical Engineering')
        session.add_all([d1, d2])

        s1 = Student(roll_no=101, name="Alice", department=d1)
        s2 = Student(roll_no=102, name="Bob", department=d1)
        s3 = Student(roll_no=103, name="Charlie", department=d2)

        session.add_all([s1, s2, s3])
        session.commit()

# -----------------------------
# DML: Data Manipulation Language
# -----------------------------

# Insert data (using DML via SQLAlchemy)
def insert_student(roll_no, name, dept_name):
    dept = session.query(Department).filter_by(name=dept_name).first()
    if dept:
        student = Student(roll_no=roll_no, name=name, department=dept)
        session.add(student)
        session.commit()
        print(f"Inserted student: {name}")
    else:
        print(f"Department {dept_name} does not exist.")

# Update data (Change student name)
def update_student_name(roll_no, new_name):
    student = session.query(Student).filter_by(roll_no=roll_no).first()
    if student:
        student.name = new_name
        session.commit()
        print(f"Updated student name to: {new_name}")
    else:
        print(f"Student with roll_no {roll_no} not found.")

# Delete data (Delete student)
def delete_student(roll_no):
    student = session.query(Student).filter_by(roll_no=roll_no).first()
    if student:
        session.delete(student)
        session.commit()
        print(f"Deleted student with roll_no {roll_no}")
    else:
        print(f"Student with roll_no {roll_no} not found.")

# -----------------------------
# Views: Creating Views in SQL
# -----------------------------

def create_view():
    with engine.connect() as conn:
        conn.execute("""
        CREATE VIEW student_view AS
        SELECT s.name AS student_name, d.name AS department_name
        FROM students s
        JOIN departments d ON s.dept_id = d.dept_id;
        """)
        print("View `student_view` created successfully.")

# Query data from view
def query_view():
    result = engine.execute("SELECT * FROM student_view")
    print("\nQuerying data from `student_view`:")
    for row in result:
        print(row)

# -----------------------------
# Embedded SQL Simulation
# -----------------------------
def embedded_sql_simulation():
    print("\nEmbedded SQL Simulation: Retrieving students from Computer Science department")
    result = engine.execute("""
        SELECT name FROM students s
        JOIN departments d ON s.dept_id = d.dept_id
        WHERE d.name = 'Computer Science'
    """)
    for row in result:
        print(row[0])

# -----------------------------
# Running the Code
# -----------------------------
if __name__ == "__main__":
    # Initialize database and create tables
    initialize_database()

    # DML: Insert a new student
    insert_student(roll_no=104, name="David", dept_name="Electrical Engineering")

    # DML: Update student's name
    update_student_name(roll_no=104, new_name="David Smith")

    # DML: Delete a student
    delete_student(roll_no=102)

    # Create and query view
    create_view()
    query_view()

    # Run Embedded SQL
    embedded_sql_simulation()
