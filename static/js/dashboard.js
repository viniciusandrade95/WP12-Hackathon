// Updated Dashboard JavaScript for Async Processing
class DashboardManager {
    constructor() {
        this.initializeElements();
        this.setupEventListeners();
        this.loadRecentAnalyses();
        this.currentProcessId = null;
        this.statusInterval = null;
    }

    initializeElements() {
        this.uploadArea = document.getElementById('uploadArea');
        this.fileInput = document.getElementById('fileInput');
        this.urlInput = document.getElementById('urlInput');
        this.analyzeBtn = document.getElementById('analyzeUrl');
        this.processingSection = document.getElementById('processingSection');
        this.progressFill = document.getElementById('progressFill');
        this.processingStatus = document.getElementById('processingStatus');
    }

    setupEventListeners() {
        // File upload events
        if (this.uploadArea) {
            this.uploadArea.addEventListener('click', () => this.fileInput.click());
            this.uploadArea.addEventListener('dragover', this.handleDragOver.bind(this));
            this.uploadArea.addEventListener('dragleave', this.handleDragLeave.bind(this));
            this.uploadArea.addEventListener('drop', this.handleDrop.bind(this));
        }
        
        if (this.fileInput) {
            this.fileInput.addEventListener('change', this.handleFileSelect.bind(this));
        }

        // URL analysis
        if (this.analyzeBtn) {
            this.analyzeBtn.addEventListener('click', this.analyzeUrl.bind(this));
        }
        
        if (this.urlInput) {
            this.urlInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.analyzeUrl();
            });
        }
    }

    handleDragOver(e) {
        e.preventDefault();
        this.uploadArea.classList.add('dragover');
    }

    handleDragLeave(e) {
        e.preventDefault();
        this.uploadArea.classList.remove('dragover');
    }

    handleDrop(e) {
        e.preventDefault();
        this.uploadArea.classList.remove('dragover');
        
        const files = e.dataTransfer.files;
        if (files.length > 0 && files[0].type === 'application/pdf') {
            this.processFile(files[0]);
        } else {
            this.showError('Please drop a valid PDF file');
        }
    }

    handleFileSelect(e) {
        const file = e.target.files[0];
        if (file && file.type === 'application/pdf') {
            this.processFile(file);
        } else {
            this.showError('Please select a valid PDF file');
        }
    }

    processFile(file) {
        const formData = new FormData();
        formData.append('file', file);
        
        this.startProcessing(`Uploading ${file.name}...`);
        this.uploadDocument(formData);
    }

    analyzeUrl() {
        const url = this.urlInput.value.trim();
        if (!url) {
            this.showError('Please enter a valid URL');
            return;
        }

        const formData = new FormData();
        formData.append('url', url);
        
        this.startProcessing('Starting URL analysis...');
        this.uploadDocument(formData);
    }

    uploadDocument(formData) {
        fetch('/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Upload response:', data);
            
            if (data.success && data.process_id) {
                this.currentProcessId = data.process_id;
                this.startStatusPolling();
            } else {
                this.showError(data.error || 'Upload failed');
            }
        })
        .catch(error => {
            console.error('Upload error:', error);
            this.showError('Upload failed: ' + error.message);
        });
    }

    startStatusPolling() {
        if (this.statusInterval) {
            clearInterval(this.statusInterval);
        }

        this.statusInterval = setInterval(() => {
            this.checkProcessingStatus();
        }, 2000); // Check every 2 seconds

        // Also check immediately
        this.checkProcessingStatus();
    }

    checkProcessingStatus() {
        if (!this.currentProcessId) return;

        fetch(`/api/status/${this.currentProcessId}`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`Status check failed: ${response.status}`);
            }
            return response.json();
        })
        .then(status => {
            console.log('Processing status:', status);
            
            // Update UI
            this.updateProcessingUI(status);
            
            // Handle completion
            if (status.status === 'completed') {
                this.handleProcessingComplete(status);
            } else if (status.status === 'failed') {
                this.handleProcessingFailed(status);
            }
        })
        .catch(error => {
            console.error('Status check error:', error);
            // Don't show error immediately, might be temporary
        });
    }

    updateProcessingUI(status) {
        // Update progress bar
        if (this.progressFill) {
            const progress = Math.max(status.progress || 0, 10); // Minimum 10%
            this.progressFill.style.width = progress + '%';
        }

        // Update status message
        if (this.processingStatus) {
            this.processingStatus.textContent = status.message || 'Processing...';
        }

        // Update step indicators based on progress
        this.updateStepIndicators(status.progress || 0);
    }

    updateStepIndicators(progress) {
        const steps = [
            { id: 'step1', threshold: 0 },
            { id: 'step2', threshold: 25 },
            { id: 'step3', threshold: 50 },
            { id: 'step4', threshold: 75 }
        ];

        steps.forEach(step => {
            const element = document.getElementById(step.id);
            if (element) {
                if (progress >= step.threshold) {
                    element.classList.add('active');
                } else {
                    element.classList.remove('active');
                }
            }
        });
    }

    handleProcessingComplete(status) {
        // Clear polling
        if (this.statusInterval) {
            clearInterval(this.statusInterval);
            this.statusInterval = null;
        }

        // Show completion
        if (this.processingStatus) {
            this.processingStatus.textContent = 'Analysis completed! Redirecting...';
        }

        // Save to recent analyses
        this.saveToRecent({
            id: status.document_id,
            company: 'Analysis Complete',
            industry: 'Unknown',
            metrics: 'Processing',
            date: new Date().toLocaleDateString()
        });

        // Redirect after short delay
        setTimeout(() => {
            window.location.href = `/results/${status.document_id}`;
        }, 1500);
    }

    handleProcessingFailed(status) {
        // Clear polling
        if (this.statusInterval) {
            clearInterval(this.statusInterval);
            this.statusInterval = null;
        }

        // Show error
        this.showError(status.error || 'Processing failed');
    }

    startProcessing(message) {
        if (this.processingStatus) {
            this.processingStatus.textContent = message;
        }
        if (this.processingSection) {
            this.processingSection.style.display = 'block';
        }
        if (this.uploadArea) {
            this.uploadArea.style.display = 'none';
        }
        this.resetProgress();
    }

    resetProgress() {
        if (this.progressFill) {
            this.progressFill.style.width = '0%';
        }
        
        document.querySelectorAll('.step').forEach(step => {
            step.classList.remove('active');
        });
        
        const step1 = document.getElementById('step1');
        if (step1) {
            step1.classList.add('active');
        }
    }

    showError(message) {
        // Clear any polling
        if (this.statusInterval) {
            clearInterval(this.statusInterval);
            this.statusInterval = null;
        }

        // Reset UI
        if (this.processingSection) {
            this.processingSection.style.display = 'none';
        }
        if (this.uploadArea) {
            this.uploadArea.style.display = 'block';
        }
        
        // Show error
        alert('Error: ' + message);
        
        // Reset process ID
        this.currentProcessId = null;
    }

    loadRecentAnalyses() {
        const recentGrid = document.getElementById('recentGrid');
        if (!recentGrid) return;
        
        // Try to load from API first
        fetch('/api/recent')
        .then(response => {
            if (response.ok) {
                return response.json();
            }
            throw new Error('Failed to load recent analyses');
        })
        .then(analyses => {
            this.displayRecentAnalyses(analyses);
        })
        .catch(error => {
            console.error('Failed to load recent analyses:', error);
            // Fallback to localStorage
            const recent = JSON.parse(localStorage.getItem('recentAnalyses') || '[]');
            this.displayRecentAnalyses(recent);
        });
    }

    displayRecentAnalyses(analyses) {
        const recentGrid = document.getElementById('recentGrid');
        if (!recentGrid) return;

        if (analyses.length === 0) {
            recentGrid.innerHTML = '<p style="color: rgba(255,255,255,0.7); text-align: center;">No recent analyses</p>';
            return;
        }

        recentGrid.innerHTML = analyses.map(analysis => `
            <div class="recent-item" onclick="window.location.href='/results/${analysis.id}'">
                <h3>${analysis.company}</h3>
                <div class="industry-badge" style="font-size: 0.8rem; margin: 0.5rem 0;">
                    ${analysis.industry}
                </div>
                <p style="color: #64748b; font-size: 0.9rem;">
                    ${analysis.metrics} metrics • ${analysis.date}
                </p>
            </div>
        `).join('');
    }

    saveToRecent(analysis) {
        let recent = JSON.parse(localStorage.getItem('recentAnalyses') || '[]');
        recent.unshift(analysis);
        recent = recent.slice(0, 6); // Keep only 6 recent
        localStorage.setItem('recentAnalyses', JSON.stringify(recent));
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new DashboardManager();
});