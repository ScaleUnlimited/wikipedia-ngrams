package com.amazon.aws.training.emr.wikipedia;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class Ngram implements WritableComparable<Ngram> {

    public static final int MIN_N = 2;
    public static final int MAX_N = 6;
    
    private char[] _ngrams;
    private int _n;
    
    // Empty constructor for deserialization
    public Ngram() {
        _n = 0;
        _ngrams = null;
    }
    
    public Ngram(int n) {
        if (n < MIN_N) {
            throw new InvalidParameterException("Less than min ngram size: " + n);
        } else if (n > MAX_N) {
            throw new InvalidParameterException("More than max ngram size: " + n);
        }
        
        _n = n;
        _ngrams = new char[_n];
        
        // Load it up with spaces initially
        for (int i = 0; i < _n; i++) {
            _ngrams[i] = ' ';
        }
    }
    
    public Ngram(char... chars) {
        this(chars.length);
        
        for (int i = 0; i < _n; i++) {
            _ngrams[i] = chars[i];
        }
    }
    
    public void shift(char newChar) {
        for (int i = 0; i < _n - 1; i++) {
            _ngrams[i] = _ngrams[i + 1];
        }
        
        _ngrams[_n - 1] = newChar;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        _n = (int)in.readByte();
        _ngrams = new char[_n];
        
        for (int i = 0; i < _n; i++) {
            _ngrams[i] = in.readChar();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(_n);
        for (int i = 0; i < _n; i++) {
            out.writeChar(_ngrams[i]);
        }
    }

    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(_ngrams);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Ngram other = (Ngram) obj;
        if (!Arrays.equals(_ngrams, other._ngrams))
            return false;
        return true;
    }

    @Override
    public int compareTo(Ngram other) {
        
        // These should be the same, but handle odd case.
        int otherN = other._n;
        
        for (int i = 0; i < Math.min(_n, otherN); i++) {
            if (_ngrams[i] < other._ngrams[i]) {
                return -1;
            } else if (_ngrams[i] > other._ngrams[i]) {
                return 1;
            }
        }
        
        if (_n < otherN) {
            return -1;
        } else if (_n > otherN) {
            return 1;
        } else {
            return 0;
        }
    }
    
    @Override
    public String toString() {
        return new String(_ngrams);
    }

}
