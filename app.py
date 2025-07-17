### 1. Main Flask App (app.py)
from flask import Flask, render_template, request, jsonify, redirect, url_for
import os
import tempfile
import requests
from werkzeug.utils import secure_filename
from core.document_processor import DocumentProcessor
from core.database import DatabaseManager
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

# Initialize components
db_manager = DatabaseManager()
processor = DocumentProcessor(
    api_key=app.config['API_KEY'],
    base_url=app.config['BASE_URL'],
    db_manager=db_manager
)

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload or URL submission"""
    try:
        # Handle file upload
        if 'file' in request.files and request.files['file'].filename:
            file = request.files['file']
            if file and file.filename.lower().endswith('.pdf'):
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filepath)
                
                # Process document
                results = processor.process_document(filepath)
                
                # Clean up
                os.remove(filepath)
                
                return jsonify({
                    'success': True,
                    'document_id': results['document_id'],
                    'redirect': url_for('results', doc_id=results['document_id'])
                })
        
        # Handle URL submission
        elif 'url' in request.form and request.form['url']:
            url = request.form['url']
            
            # Download PDF from URL
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Save temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                tmp_file.write(response.content)
                tmp_filepath = tmp_file.name
            
            try:
                # Process document
                results = processor.process_document(tmp_filepath)
                
                return jsonify({
                    'success': True,
                    'document_id': results['document_id'],
                    'redirect': url_for('results', doc_id=results['document_id'])
                })
            finally:
                os.unlink(tmp_filepath)
        
        return jsonify({'success': False, 'error': 'No valid file or URL provided'})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/results/<int:doc_id>')
def results(doc_id):
    """Display analysis results"""
    try:
        intelligence = processor.get_company_intelligence(doc_id)
        return render_template('results.html', data=intelligence, doc_id=doc_id)
    except Exception as e:
        return render_template('results.html', error=str(e))

@app.route('/api/progress/<int:doc_id>')
def get_progress(doc_id):
    """Get processing progress"""
    progress = processor.get_processing_progress(doc_id)
    return jsonify(progress)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
