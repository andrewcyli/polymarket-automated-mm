#!/bin/bash
# PolyMaker Bot Installation Script

set -e
echo "=========================================="
echo "PolyMaker Bot Installation"
echo "=========================================="
echo ""
echo "Checking Python version..."
python3 --version
if [ -d "venv" ]; then
    echo "Virtual environment already exists"
else
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "Virtual environment created"
fi
echo "Activating virtual environment..."
source venv/bin/activate
echo "Upgrading pip..."
pip install --upgrade pip
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt
echo ""
echo "=========================================="
echo "Installation complete!"
echo "=========================================="
echo ""
echo "To run the bot:"
echo "  1. source venv/bin/activate"
echo "  2. python main.py"
echo ""
