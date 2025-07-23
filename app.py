# app_refactored.py (Fixed for older Python versions)
"""
Refactored Flask application with resolved inconsistencies - Python 3.7+ compatible
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import os
import tempfile
import requests
from werkzeug.utils import secure_filename
from pathlib import Path
import threading
import uuid
from datetime import datetime

# Import refactored components
from core.document_processor import DocumentProcessor
from core.database import DatabaseManager
from utils.api_client import LLMClient
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

# Initialize components with proper dependency injection
db_manager = DatabaseManager(app.config.get('DATABASE_PATH', 'intelligent_rag.db'))
llm_client = LLMClient(app.config['API_KEY'], app.config['BASE_URL'])
processor = DocumentProcessor(llm_client, db_manager)

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

# Add this to your app.py
processing_status = {}  # In-memory storage for processing status

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload with asynchronous processing"""
    try:
        # Generate unique processing ID
        process_id = str(uuid.uuid4())
        
        # Handle file upload
        if 'file' in request.files and request.files['file'].filename:
            file = request.files['file']
            if file and file.filename.lower().endswith('.pdf'):
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(filepath)
                
                # Initialize processing status
                processing_status[process_id] = {
                    'status': 'processing',
                    'progress': 0,
                    'message': 'Starting analysis...',
                    'document_id': None,
                    'error': None,
                    'created_at': datetime.now()
                }
                
                # Start background processing
                thread = threading.Thread(
                    target=process_document_async,
                    args=(filepath, process_id)
                )
                thread.daemon = True
                thread.start()
                
                return jsonify({
                    'success': True,
                    'process_id': process_id,
                    'status': 'processing',
                    'message': 'Document upload successful. Processing started...'
                })
        
        # Handle URL submission
        elif 'url' in request.form and request.form['url']:
            url = request.form['url']
            process_id = str(uuid.uuid4())
            
            # Initialize processing status
            processing_status[process_id] = {
                'status': 'downloading',
                'progress': 0,
                'message': 'Downloading PDF...',
                'document_id': None,
                'error': None,
                'created_at': datetime.now()
            }
            
            # Start background processing
            thread = threading.Thread(
                target=process_url_async,
                args=(url, process_id)
            )
            thread.daemon = True
            thread.start()
            
            return jsonify({
                'success': True,
                'process_id': process_id,
                'status': 'processing',
                'message': 'Download started. Processing will begin shortly...'
            })
        
        return jsonify({
            'success': False, 
            'error': 'No valid file or URL provided'
        })
        
    except Exception as e:
        app.logger.error(f"Upload error: {str(e)}")
        return jsonify({
            'success': False, 
            'error': 'An unexpected error occurred during upload'
        })

def process_document_async(filepath, process_id):
    """Process document in background thread"""
    try:
        # Update status
        processing_status[process_id].update({
            'status': 'processing',
            'progress': 10,
            'message': 'Analyzing document structure...'
        })
        
        # Process document
        results = processor.process_document(filepath)
        
        if results.get('success'):
            processing_status[process_id].update({
                'status': 'completed',
                'progress': 100,
                'message': 'Analysis completed successfully!',
                'document_id': results['document_id']
            })
        else:
            processing_status[process_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'Analysis failed',
                'error': results.get('error', 'Processing failed')
            })
            
    except Exception as e:
        processing_status[process_id].update({
            'status': 'failed',
            'progress': 0,
            'message': 'Processing failed',
            'error': str(e)
        })
    finally:
        # Clean up file
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except:
                pass

def process_url_async(url, process_id):
    """Process URL in background thread"""
    try:
        # Update status
        processing_status[process_id].update({
            'status': 'downloading',
            'progress': 5,
            'message': 'Downloading PDF from URL...'
        })
        
        # Download PDF
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Validate content type
        content_type = response.headers.get('content-type', '')
        if 'pdf' not in content_type.lower():
            processing_status[process_id].update({
                'status': 'failed',
                'error': 'URL does not point to a PDF file'
            })
            return
        
        # Save temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            tmp_file.write(response.content)
            tmp_filepath = tmp_file.name
        
        # Update status
        processing_status[process_id].update({
            'status': 'processing',
            'progress': 15,
            'message': 'Download complete. Starting analysis...'
        })
        
        # Process document
        results = processor.process_document(tmp_filepath)
        
        if results.get('success'):
            processing_status[process_id].update({
                'status': 'completed',
                'progress': 100,
                'message': 'Analysis completed successfully!',
                'document_id': results['document_id']
            })
        else:
            processing_status[process_id].update({
                'status': 'failed',
                'progress': 0,
                'message': 'Analysis failed',
                'error': results.get('error', 'Processing failed')
            })
            
    except Exception as e:
        processing_status[process_id].update({
            'status': 'failed',
            'progress': 0,
            'message': 'Processing failed',
            'error': str(e)
        })
    finally:
        # Clean up temporary file
        if 'tmp_filepath' in locals() and os.path.exists(tmp_filepath):
            try:
                os.unlink(tmp_filepath)
            except:
                pass

@app.route('/api/status/<process_id>')
def get_processing_status(process_id):
    """Get real-time processing status"""
    if process_id not in processing_status:
        return jsonify({'error': 'Process not found'}), 404
    
    status = processing_status[process_id]
    
    # Clean up old completed processes (older than 1 hour)
    if status['status'] in ['completed', 'failed']:
        age = datetime.now() - status['created_at']
        if age.total_seconds() > 3600:  # 1 hour
            del processing_status[process_id]
    
    return jsonify(status)


@app.route('/results/<int:doc_id>')
def results(doc_id):
    """Display analysis results with proper error handling"""
    try:
        intelligence = processor.get_company_intelligence(doc_id)
        
        if 'error' in intelligence:
            return render_template('results.html', 
                                 error=intelligence['error'], 
                                 doc_id=doc_id)
        
        return render_template('results.html', 
                             data=intelligence, 
                             doc_id=doc_id)
                             
    except Exception as e:
        app.logger.error(f"Results error for doc {doc_id}: {str(e)}")
        return render_template('results.html', 
                             error="Failed to load analysis results", 
                             doc_id=doc_id)

@app.route('/api/progress/<int:doc_id>')
def get_progress(doc_id):
    """Get processing progress for real-time updates"""
    try:
        progress = processor.get_processing_progress(doc_id)
        return jsonify(progress)
    except Exception as e:
        app.logger.error(f"Progress error for doc {doc_id}: {str(e)}")
        return jsonify({
            'error': 'Failed to get progress',
            'status': 'error'
        })

@app.route('/api/recent')
def get_recent_analyses():
    """Get recent analyses for dashboard"""
    try:
        recent = processor.get_recent_analyses(limit=6)
        return jsonify(recent)
    except Exception as e:
        app.logger.error(f"Recent analyses error: {str(e)}")
        return jsonify([])

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', 
                         error="Page not found", 
                         code=404), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', 
                         error="Internal server error", 
                         code=500), 500


@app.route('/test-extraction')
def test_extraction():
    """Quick test endpoint"""
    test_text = """
    Total revenue for 2024 was €2,456.7 million, representing a growth of 15.3%.
    Net income reached €345.2 million. The company has 12,500 employees.
    Operating costs were €1,890.5 million.
    """
    
    metrics = processor.llm_client.extract_metrics(
        test_text, 1, 
        "Extract metrics as JSON array: [{\"metric_name\": \"name\", \"value\": number, \"unit\": \"unit\", \"period\": \"period\"}]",
        30, "test"
    )
    
    return jsonify({
        "test_text": test_text,
        "metrics_found": len(metrics),
        "metrics": metrics
    })

@app.route('/test-metrics')
def test_metrics():
    """Test if metrics extraction is working"""
    cursor = db_manager.connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) as total, 
               MAX(created_at) as latest 
        FROM financial_metrics
    """)
    result = cursor.fetchone()
    
    return jsonify({
        "total_metrics_in_db": result[0],
        "last_extraction": str(result[1]) if result[1] else "Never"
    })


# Add this to your app.py for debugging

@app.route('/debug/<int:doc_id>')
def debug_document(doc_id):
    """Debug endpoint to see what's in the database"""
    cursor = db_manager.connection.cursor()
    
    # Get all metrics for this document
    cursor.execute("""
        SELECT 
            fm.metric_name,
            fm.metric_type,
            fm.value,
            fm.unit,
            fm.period,
            fm.confidence,
            fm.page_number,
            mv.verification_status
        FROM financial_metrics fm
        LEFT JOIN metric_verification mv ON fm.id = mv.metric_id
        WHERE fm.document_id = ?
        ORDER BY fm.page_number, fm.metric_name
    """, (doc_id,))
    
    metrics = cursor.fetchall()
    
    # Group by type
    by_type = {}
    for m in metrics:
        metric_type = m[1] or 'unknown'
        if metric_type not in by_type:
            by_type[metric_type] = []
        by_type[metric_type].append({
            'name': m[0],
            'value': m[2],
            'unit': m[3],
            'period': m[4],
            'confidence': m[5],
            'page': m[6],
            'verification': m[7] or 'none'
        })
    
    # Get insights
    cursor.execute("""
        SELECT concept, insight_text, confidence
        FROM business_intelligence
        WHERE document_id = ?
    """, (doc_id,))
    
    insights = cursor.fetchall()
    
    return jsonify({
        'document_id': doc_id,
        'total_metrics': len(metrics),
        'metrics_by_type': by_type,
        'type_counts': {k: len(v) for k, v in by_type.items()},
        'insights_count': len(insights),
        'insights': [{'concept': i[0], 'text': i[1], 'confidence': i[2]} for i in insights]
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)