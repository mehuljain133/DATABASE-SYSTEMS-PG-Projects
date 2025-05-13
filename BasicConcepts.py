# Unit-I Basic Concepts: Data modeling for a database, abstraction and data integration, three level architecture of a DBMS. 

pip install sqlalchemy

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# -----------------------------
# INTERNAL LEVEL: Physical Schema and Engine Setup
# -----------------------------
Base = declarative_base()
engine = create_engine("sqlite:///university.db", echo=True)
Session = sessionmaker(bind=engine)
session = Session()

# -----------------------------
# CONCEPTUAL LEVEL: Data Modeling
# -----------------------------

# Department Entity
class Department(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    students = relationship("Student", back_populates="department")

# Student Entity
class Student(Base):
    __tablename__ = 'students'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    department_id = Column(Integer, ForeignKey('departments.id'))

    department = relationship("Department", back_populates="students")

# -----------------------------
# Abstraction and Integration
# -----------------------------
def initialize_database():
    Base.metadata.create_all(engine)

    # Sample data insertion (if empty)
    if not session.query(Department).first():
        cs = Department(name="Computer Science")
        ee = Department(name="Electrical Engineering")
        session.add_all([cs, ee])
        session.commit()

        students = [
            Student(name="Alice", department=cs),
            Student(name="Bob", department=ee),
            Student(name="Charlie", department=cs)
        ]
        session.add_all(students)
        session.commit()

# -----------------------------
# EXTERNAL LEVEL: User Interaction
# -----------------------------
def display_students():
    print("\nList of Students and Departments:")
    students = session.query(Student).all()
    for student in students:
        print(f"{student.name} ({student.department.name})")

# -----------------------------
# Run Everything
# -----------------------------
if __name__ == "__main__":
    initialize_database()
    display_students()
