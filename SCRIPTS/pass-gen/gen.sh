#!/bin/bash

# Check if the input parameter is provided
if [ $# -eq 0 ]; then
    echo "No input parameter provided."
    echo "Usage: $0 'password'"
    exit 1
fi

# Input parameter
INPUT_PARAM="$1"

# Java source and class files
JAVA_FILE="PasswordUtils.java"
CLASS_FILE="PasswordUtils.class"

# Compile program
javac "$JAVA_FILE"

# Run the Java program with the input parameter
java PasswordUtils "$INPUT_PARAM"
