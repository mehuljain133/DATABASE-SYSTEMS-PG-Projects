# Unit-II Database Design: Entity Relationship model, Extended Entity Relationship model.

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# -------------------------------
# SETUP: DB and ORM Base
# -------------------------------
Base = declarative_base()
engine = create_engine("sqlite:///university_eer.db", echo=False)
Session = sessionmaker(bind=engine)
session = Session()

# -------------------------------
# ER MODEL: Department & Course
# -------------------------------
class Department(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    
    courses = relationship("Course", back_populates="department")

class Course(Base):
    __tablename__ = 'courses'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    department_id = Column(Integer, ForeignKey('departments.id'))

    department = relationship("Department", back_populates="courses")
    students = relationship("Student", secondary="enrollments", back_populates="courses")
    professor_id = Column(Integer, ForeignKey("professors.id"))

# -------------------------------
# EER MODEL: Person (Supertype)
# -------------------------------
class Person(Base):
    __tablename__ = 'persons'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'person'
    }

# EER Specialization: Student
class Student(Person):
    __tablename__ = 'students'
    id = Column(Integer, ForeignKey('persons.id'), primary_key=True)
    major = Column(String)

    courses = relationship("Course", secondary="enrollments", back_populates="students")

    __mapper_args__ = {
        'polymorphic_identity': 'student',
    }

# EER Specialization: Professor
class Professor(Person):
    __tablename__ = 'professors'
    id = Column(Integer, ForeignKey('persons.id'), primary_key=True)
    title = Column(String)

    courses = relationship("Course", backref="professor")

    __mapper_args__ = {
        'polymorphic_identity': 'professor',
    }

# -------------------------------
# Relationship Table: Enrolls (M:N)
# -------------------------------
enrollments = Table('enrollments', Base.metadata,
    Column('student_id', ForeignKey('students.id'), primary_key=True),
    Column('course_id', ForeignKey('courses.id'), primary_key=True)
)

# -------------------------------
# DB Initialization and Sample Data
# -------------------------------
def initialize_database():
    Base.metadata.create_all(engine)

    if not session.query(Department).first():
        # Create Departments
        cs = Department(name="Computer Science")
        math = Department(name="Mathematics")
        session.add_all([cs, math])

        # Create Courses
        c1 = Course(name="Algorithms", department=cs)
        c2 = Course(name="Linear Algebra", department=math)
        session.add_all([c1, c2])

        # Create People
        alice = Student(name="Alice", major="Computer Science")
        bob = Student(name="Bob", major="Mathematics")
        prof_smith = Professor(name="Dr. Smith", title="Associate Professor")

        # Relationships
        c1.students.append(alice)
        c2.students.append(bob)
        prof_smith.courses.append(c1)

        session.add_all([alice, bob, prof_smith])
        session.commit()

# -------------------------------
# Output for Demonstration
# -------------------------------
def display_data():
    print("\n--- Courses and Professors ---")
    for course in session.query(Course).all():
        print(f"{course.name} - Taught by: {course.professor.name if course.professor else 'N/A'}")

    print("\n--- Student Enrollments ---")
    for student in session.query(Student).all():
        print(f"{student.name} enrolled in: {[c.name for c in student.courses]}")

# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    initialize_database()
    display_data()
