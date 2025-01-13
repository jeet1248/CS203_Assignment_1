import os
import json
import logging
import time
from logging.handlers import RotatingFileHandler
from flask import Flask, render_template, request, redirect, url_for, flash, session
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider, Status, StatusCode
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.trace import SpanKind

# Flask App Initialization
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "supersecretkey")
COURSE_FILE = 'course_catalog.json'
LOG_FILE = 'logs.json'

# OpenTelemetry Setup
resource = Resource.create({"service.name": "course-catalog-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

# Creating Jaeger Exporter
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)

# Creating ConsoleSpanExporter
console_exporter = ConsoleSpanExporter()

# Configuring the BatchSpanProcessor for both Jaeger and Console Exporters
span_processor_jaeger = BatchSpanProcessor(jaeger_exporter)
span_processor_console = BatchSpanProcessor(console_exporter)

# Adding both processors to the tracer provider
trace.get_tracer_provider().add_span_processor(span_processor_jaeger)
trace.get_tracer_provider().add_span_processor(span_processor_console)
FlaskInstrumentor().instrument_app(app)

# Logger Setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=1000000, backupCount=3)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Utility Functions
def load_courses():
    """Load courses from the JSON file and track the operation with OpenTelemetry tracing."""

    # Start timing for performance measurement
    start_time = time.time()

    # Starting a new span for tracing the 'load-courses' operation
    with tracer.start_as_current_span("load-courses", kind=SpanKind.INTERNAL) as span:
        
        # Setting attributes related to this span
        span.set_attribute("operation", "load_courses")
        span.set_attribute("http.url", request.url)

        # Checking if the course file exists
        if not os.path.exists(COURSE_FILE):
            span.set_attribute("file.exists", False)
            return []

        # Loading the courses from the JSON file
        with open(COURSE_FILE, 'r') as file:
            courses = json.load(file)

        # Setting the course count in the span attributes for tracking
        span.set_attribute("course.count", len(courses))

        # End timing after loading the courses
        end_time = time.time()
        span.set_attribute("load.time", end_time - start_time)

        # Logging the event of loading courses with details
        logger.info(json.dumps({
            "event": "load-courses",
            "http.url": request.url,
            "course.count": len(courses),
            "load.time": end_time - start_time
        }))

    # Returning the loaded courses
    return courses

def save_courses(data):
    """Save new course data to the JSON file and track the operation with OpenTelemetry tracing."""

    # Start timing for performance measurement
    start_time = time.time()

    # Starting a new span for tracing the 'save-courses' operation
    with tracer.start_as_current_span("save-courses", kind=SpanKind.INTERNAL) as span:
        
        # Setting attributes related to this span
        span.set_attribute("operation", "save_courses")
        span.set_attribute("http.url", request.url)

        # Loading the existing courses from the JSON file
        courses = load_courses()

        # Appending the new course data to the list of courses
        courses.append(data)

        # Opening the course file and saving the updated course list
        with open(COURSE_FILE, 'w') as file:
            json.dump(courses, file, indent=4)

        # End timing after saving the courses
        end_time = time.time()

        # Setting the time taken for the save operation in the span
        span.set_attribute("save.time", end_time - start_time)

        # Logfing the event of saving the courses with details
        logger.info(json.dumps({
            "event": "save-courses",
            "http.url": request.url,
            "save.time": end_time - start_time
        }))


def validate_course(data):
    """Validate course data and return error or warning messages with tracing and logging."""

    # Start timing for performance measurement
    start_time = time.time()

    # Starting a new span for tracing the 'validate-course' operation
    with tracer.start_as_current_span("validate-course", kind=SpanKind.INTERNAL) as span:
        
        # Setting span attributes related to this operation
        span.set_attribute("operation", "validate_course")
        span.set_attribute("http.url", request.url)

        # Defining required fields for course validation
        required_fields = ['code', 'name', 'instructor']

        # Checking for empty required fields
        empty_fields = [field for field in required_fields if not data.get(field, "").strip()]

        # If required fields are missing, logging and returning error
        if empty_fields:
            span.set_attribute("validation.status", "error")
            span.set_attribute("missing_fields", ", ".join(empty_fields))

            # End timing for the validation process
            end_time = time.time()
            span.set_attribute("validation.time", end_time - start_time)

            # Logging the validation error event
            logger.error(json.dumps({
                "event": "validate-course-error",
                "http.url": request.url,
                "missing_fields": ", ".join(empty_fields),
                "validation.time": end_time - start_time
            }))

            # Returning error message for missing fields
            return False, f"Error: Missing required fields - {', '.join(empty_fields)}"

        # Checking for empty fields that are not required (for warnings)
        warning_fields = [field for field in data if not data.get(field, "").strip()]

        # If there are warning fields, logging and returning warning
        if warning_fields:
            span.set_attribute("validation.status", "warning")
            span.set_attribute("warning_fields", ", ".join(warning_fields))

            # End timing for the validation process
            end_time = time.time()
            span.set_attribute("validation.time", end_time - start_time)

            # Logging the validation warning event
            logger.warning(json.dumps({
                "event": "validate-course-warning",
                "http.url": request.url,
                "warning_fields": ", ".join(warning_fields),
                "validation.time": end_time - start_time
            }))

            # Returning warning message for empty fields
            return "warning", f"Warning: Fields {', '.join(warning_fields)} are empty"

        # If no errors or warnings, marking validation as successful
        end_time = time.time()
        span.set_attribute("validation.status", "success")
        span.set_attribute("validation.time", end_time - start_time)

        # Logging the successful validation event
        logger.info(json.dumps({
            "event": "validate-course-success",
            "http.url": request.url,
            "validation.time": end_time - start_time
        }))

        # Returning success message for validation
        return True, "Validation successful"

def delete_course_by_code(code):
    """Delete the course by its code with tracing and logging."""

    # Start timing for performance measurement
    start_time = time.time()

    # Starting a new span for tracing the 'delete-course-by-code' operation
    with tracer.start_as_current_span("delete-course-by-code", kind=SpanKind.INTERNAL) as span:
        
        # Setting span attributes related to this operation
        span.set_attribute("operation", "delete_course_by_code")
        span.set_attribute("http.url", request.url)

        # Loading the current list of courses
        courses = load_courses()

        # Finding the course to delete based on the provided code
        course_to_delete = next((c for c in courses if c['code'] == code), None)

        # If the course is found, deleting it and updating the JSON file
        if course_to_delete:
            courses = [c for c in courses if c['code'] != code]
            with open(COURSE_FILE, 'w') as file:
                json.dump(courses, file, indent=4)

            # End timing for the deletion process
            end_time = time.time()
            span.set_attribute("delete.time", end_time - start_time)

            # Logging the deletion event
            logger.info(json.dumps({
                "event": "course-deleted",
                "course.code": code,
                "http.url": request.url,
                "delete.time": end_time - start_time
            }))

            # Returning success if the course was deleted
            return True
        else:
            # If course not found, logging a warning and returning false
            span.set_attribute("delete.status", "not-found")
            end_time = time.time()
            span.set_attribute("delete.time", end_time - start_time)

            # Logging the course not found event
            logger.warning(json.dumps({
                "event": "course-delete-not-found",
                "course.code": code,
                "http.url": request.url,
                "delete.time": end_time - start_time
            }))

            # Returning false if the course was not found
            return False
        
          
# Route for the Index page
@app.route('/')
def index():
    """Render the index page with tracing, logging, and performance monitoring."""

    # Starting the tracing span for the render-index operation
    with tracer.start_as_current_span("render-index", kind=SpanKind.SERVER) as span:
        
        # Setting span attributes related to the request and operation
        span.set_attribute("http.method", request.method)
        span.set_attribute("http.url", request.url)
        span.set_attribute("user.ip", request.remote_addr)

        # Start timing to measure the render time
        start_time = time.time()

        # Rendering the template for the index page
        response = render_template('index.html')

        # End timing after rendering is complete
        end_time = time.time()

        # Setting span attributes for the render time
        span.set_attribute("render.time", end_time - start_time)

        # Logging the details of the rendering operation
        logger.info(json.dumps({
            "event": "render-index",
            "http.method": request.method,
            "http.url": request.url,
            "user.ip": request.remote_addr,
            "render.time": end_time - start_time
        }))

    # Returning the rendered response
    return response

# Route for adding a course
@app.route('/add_course', methods=['GET', 'POST'])
def add_course():
    """Handle the addition of a new course with validation, error handling, and logging."""

    # Processing POST request
    if request.method == 'POST':
        
        # Starting tracing the add-course operation
        with tracer.start_as_current_span("add-course", kind=SpanKind.SERVER) as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", request.url)
            span.set_attribute("user.ip", request.remote_addr)

            # Initializing error counts and course count in the session if not already present
            if 'missing_field_errors' not in session:
                session['missing_field_errors'] = 0
            if 'validation_errors' not in session:
                session['validation_errors'] = 0
            if 'database_errors' not in session:
                session['database_errors'] = 0
            if 'added_courses_count' not in session:
                session['added_courses_count'] = 0  # Initialize added courses count

            # Getting error counts from session
            missing_field_errors = session['missing_field_errors']
            validation_errors = session['validation_errors']
            database_errors = session['database_errors']
            added_courses_count = session['added_courses_count']

            # Creating a course object from the form data
            course = {
                'code': request.form['code'],
                'name': request.form['name'],
                'instructor': request.form['instructor'],
                'semester': request.form['semester'],
                'schedule': request.form['schedule'],
                'classroom': request.form['classroom'],
                'prerequisites': request.form['prerequisites'],
                'grading': request.form['grading'],
                'description': request.form['description']
            }

            # Checking for missing required fields
            required_fields = ['code', 'name', 'instructor']
            empty_fields = [field.capitalize() for field in required_fields if not course.get(field).strip()]

            # If there are missing required fields, logging and flashing an error
            if empty_fields:
                missing_field_errors += 1
                fields = ", ".join(empty_fields)
                flash(f"The following fields are required and cannot be empty: {fields}.", "error")
                session['missing_field_errors'] = missing_field_errors  # Update session error count
                span.set_attribute("error.missing_fields", missing_field_errors)
                return render_template('add_course.html', course=course)

            # Validating the course data
            validation_start_time = time.time()
            valid, message = validate_course(course)
            validation_end_time = time.time()
            span.set_attribute("validation.time", validation_end_time - validation_start_time)

            # If validation fails, logging and flashing an error
            if not valid:
                validation_errors += 1
                logger.error(json.dumps({
                    "event": "course-add-error",
                    "error.message": message,
                    "http.method": request.method,
                    "http.url": request.url,
                    "user.ip": request.remote_addr
                }))
                # Updating session error count
                session['validation_errors'] = validation_errors
                span.set_status(Status(StatusCode.ERROR, message))
                span.set_attribute("error.validation", validation_errors)
                flash(f"Validation error: {message}", "error")
                return render_template('add_course.html', course=course)

            # Saving the course data
            save_start_time = time.time()
            try:
                save_courses(course)
                added_courses_count += 1
                # Updating session count
                session['added_courses_count'] = added_courses_count
            except Exception as e:
                database_errors += 1
                logger.error(json.dumps({
                    "event": "course-save-error",
                    "error.message": str(e),
                    "http.method": request.method,
                    "http.url": request.url,
                    "user.ip": request.remote_addr
                }))
                # Updating session error count
                session['database_errors'] = database_errors
                span.set_status(Status(StatusCode.ERROR, "Error while saving the course"))
                span.set_attribute("error.database", database_errors)
                flash("Failed to save course", "error")
                return render_template('add_course.html', course=course)

            # End timing for saving course data
            save_end_time = time.time()
            span.set_attribute("save.time", save_end_time - save_start_time)

            # Logging the error and validation counts as attributes in the span
            span.set_attribute("error.missing_fields", missing_field_errors)
            span.set_attribute("error.validation", validation_errors)
            span.set_attribute("error.database", database_errors)
            span.set_attribute("added_courses.count", added_courses_count)

            # Logging the successful course addition
            logger.info(json.dumps({
                "event": "course-added",
                "http.method": request.method,
                "http.url": request.url,
                "user.ip": request.remote_addr,
                "save.time": save_end_time - save_start_time
            }))

            # Flashing a success message and redirecting to the course catalog
            flash("Course added successfully", "success")
            return redirect(url_for('course_catalog'))

    # Rendering the add_course page
    return render_template('add_course.html')


# Route for rendering the course catalog
@app.route('/catalog', methods=['GET', 'POST'])
def course_catalog():
    """Render the course catalog with performance monitoring and session handling."""

    # Start tracing the render-course-catalog operation
    with tracer.start_as_current_span("render-course-catalog", kind=SpanKind.SERVER) as span:
        # Adding HTTP-related information to the span
        span.set_attribute("http.method", request.method)
        span.set_attribute("http.url", request.url)
        span.set_attribute("user.ip", request.remote_addr)

        # Initializing catalog page access count in the session if not already present
        if 'catalog_page_access_count' not in session:
            session['catalog_page_access_count'] = 0
        
        # Getting the current catalog page access count and incrementing it
        catalog_page_access_count = session['catalog_page_access_count']
        catalog_page_access_count += 1 
        session['catalog_page_access_count'] = catalog_page_access_count

        # Adding the catalog page access count as an attribute to the span
        span.set_attribute("catalog.page_access_count", catalog_page_access_count)

        # Loading the courses from the data source and tracing the load time after calculating
        course_load_start_time = time.time()
        courses = load_courses()
        course_load_end_time = time.time()
        span.set_attribute("course_loading.time", course_load_end_time - course_load_start_time)

        # Rendering the page and track processing time
        page_render_start_time = time.time()
        span.set_attribute("courses.count", len(courses))
        response = render_template('course_catalog.html', courses=courses)
        page_render_end_time = time.time()
        span.set_attribute("processing.time", page_render_end_time - page_render_start_time)

        # Logging the performance data, including loading and rendering times
        logger.info(json.dumps({
            "event": "render-course-catalog",
            "http.method": request.method,
            "http.url": request.url,
            "user.ip": request.remote_addr,
            "course_loading.time": course_load_end_time - course_load_start_time,
            "processing.time": page_render_end_time - page_render_start_time
        }))

    return response

# Route for rendering course details
@app.route('/course/<code>')
def course_details(code):
    """Render the details of a specific course, with performance monitoring and error handling."""

    # Start tracing the course-details operation
    with tracer.start_as_current_span("course-details", kind=SpanKind.SERVER) as span:
        # Adding HTTP-related information and course code to the span
        span.set_attribute("http.method", request.method)
        span.set_attribute("http.url", request.url)
        span.set_attribute("user.ip", request.remote_addr)
        span.set_attribute("course.code", code)

        # Start timing the course load operation and tracing it
        course_load_start_time = time.time()
        courses = load_courses()
        course = next((c for c in courses if c['code'] == code), None)
        course_load_end_time = time.time()
        span.set_attribute("course_loading.time", course_load_end_time - course_load_start_time)

        # Handling case where the course is not found
        if course is None:
            logger.warning(json.dumps({
                "event": "course-not-found",
                "course.code": code,
                "http.method": request.method,
                "http.url": request.url
            }))
            # Redirecting to the course catalog if the course is not found
            return redirect(url_for('course_catalog'))

        # Logging the event when a course detail page is viewed
        logger.info(json.dumps({
            "event": "course-details-viewed",
            "course.code": code,
            "http.method": request.method,
            "http.url": request.url
        }))

        # Rendering the course details page
        return render_template('course_details.html', course=course)
    
# Route for deleting a course
@app.route('/delete_course/<code>', methods=['POST'])
def delete_course(code):
    """Delete a specific course by its code, with performance monitoring and error handling."""

    # Start tracing the delete-course operation
    with tracer.start_as_current_span("delete-course", kind=SpanKind.SERVER) as span:
        # Adding HTTP-related information and course code to the span
        span.set_attribute("http.method", request.method)
        span.set_attribute("http.url", request.url)
        span.set_attribute("user.ip", request.remote_addr)
        span.set_attribute("course.code", code)

        # Initializing error count for deletion
        deletion_errors = 0

        # Calculate timing the course deletion process
        course_delete_start_time = time.time()
        success = delete_course_by_code(code)
        course_delete_end_time = time.time()

        # Logging the course deletion time in the span
        span.set_attribute("course_deletion.time", course_delete_end_time - course_delete_start_time)

        # Handling success or failure of the course deletion
        if success:
            flash(f"Course with code {code} deleted successfully.", 'success')
            logger.info(json.dumps({
                "event": "course-deleted",
                "course.code": code,
                "http.method": request.method,
                "http.url": request.url,
                "user.ip": request.remote_addr,
                "course_deletion.time": course_delete_end_time - course_delete_start_time
            }))
        else:
            deletion_errors += 1
            flash(f"Course with code {code} not found.", 'danger')
            logger.warning(json.dumps({
                "event": "course-deletion-failed",
                "course.code": code,
                "http.method": request.method,
                "http.url": request.url,
                "user.ip": request.remote_addr
            }))
            span.set_attribute("error.deletion", deletion_errors)

        # Adding error deletion count to the span
        span.set_attribute("error.deletion", deletion_errors)

    # Redirecting to the course catalog after the deletion attempt
    return redirect(url_for('course_catalog'))

if __name__ == "__main__":
    app.run(debug=True)
