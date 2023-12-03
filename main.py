from fastapi import Depends, FastAPI, Request, File, UploadFile, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import csv
import io
import logging
from fastapi.staticfiles import StaticFiles

app = FastAPI(debug=True)

app.mount("/static", StaticFiles(directory="static"), name="static")

DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    age = Column(Integer)

Base.metadata.create_all(bind=engine)

# Template setup
templates = Jinja2Templates(directory="templates")

# Dependency to get a database session
def get_db():
    try:
        db = sessionmaker(autocommit=False, autoflush=False, bind=engine)()
        yield db
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")
    finally:
        db.close()

# FastAPI routes
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: Session = Depends(get_db)):
    users = db.query(User).all()
    return templates.TemplateResponse("index.html", {"request": request, "users": users})

@app.post("/uploadfile/")
async def create_upload_file(
    file: UploadFile = File(...),
    id_col: int = 0,
    name_col: int = 1,
    age_col: int = 2,
    db: Session = Depends(get_db)
):
    try:
        # Read CSV file
        contents = await file.read()

        # Use io.StringIO to handle decoding and splitting lines
        decoded_contents = io.StringIO(contents.decode("utf-8"))
        csv_reader = csv.reader(decoded_contents)

        # Skip the header row
        header = next(csv_reader)
        raw_limit =50

        for row_num, row in enumerate(csv_reader, start=2):  # Start from the second row
            try:
                if row_num > raw_limit + 1:  # Adding 1 to account for the header row

                    # Validate column indices
                    if id_col >= len(row) or name_col >= len(row) or age_col >= len(row):
                        raise ValueError("Invalid column indices")

                    id = int(row[id_col])
                    name = row[name_col]
                    age = int(row[age_col])

                    # Check if a user with the same ID already exists
                    existing_user = db.query(User).filter(User.id == id).first()

                    if existing_user:
                        # Log a warning
                        logging.warning(f"User with ID {id} already exists. Updating data for row {row_num}.")

                        # Update existing user's data with new values
                        existing_user.name = name
                        existing_user.age = age
                    else:
                        # Create a new user
                        user = User(id=id, name=name, age=age)
                        db.add(user)
            except ValueError as ve:
                # Log the error and continue processing the next row
                logging.error(f"Error converting values to integers in row {row_num}: {ve}")

        db.commit()

    except Exception as e:
        # Log the exception and handle it appropriately
        logging.error(f"Error processing CSV: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Error processing CSV")

    # Redirect to the result_page endpoint using status code 303
    return RedirectResponse(url="/result_page/", status_code=303)

@app.get("/result_page/", response_class=HTMLResponse)
@app.post("/result_page/", response_class=HTMLResponse)
async def result_page(request: Request, db: Session = Depends(get_db)):
    if request.method == "GET":
        users = db.query(User).all()
        return templates.TemplateResponse("result.html", {"request": request, "users": users})
    elif request.method == "POST":
        # Handle POST requests if needed
        pass
    else:
        raise HTTPException(status_code=405, detail="Method Not Allowed")

if __name__ == "__main__":
    import uvicorn

    # Run the FastAPI app using Uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
