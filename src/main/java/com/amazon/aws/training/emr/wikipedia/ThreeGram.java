package com.amazon.aws.training.emr.wikipedia;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ThreeGram implements WritableComparable<ThreeGram> {

    private char[] _threeGram;
    
    public ThreeGram() {
        _threeGram = new char[3];
    }
    
    public ThreeGram(char c0, char c1, char c2) {
        this();
        
        _threeGram[0] = c0;
        _threeGram[1] = c1;
        _threeGram[2] = c2;
    }
    
    public void shift(char newChar) {
        _threeGram[0] = _threeGram[1];
        _threeGram[1] = _threeGram[2];
        _threeGram[2] = newChar;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        _threeGram[0] = in.readChar();
        _threeGram[1] = in.readChar();
        _threeGram[2] = in.readChar();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChar(_threeGram[0]);
        out.writeChar(_threeGram[1]);
        out.writeChar(_threeGram[2]);
    }

    @Override
    public int compareTo(ThreeGram other) {
        if (_threeGram[0] < other._threeGram[0]) {
            return -1;
        } else if (_threeGram[0] > other._threeGram[0]) {
            return 1;
        } else if (_threeGram[1] < other._threeGram[1]) {
            return -1;
        } else if (_threeGram[1] > other._threeGram[1]) {
            return 1;
        } else if (_threeGram[2] < other._threeGram[2]) {
            return -1;
        } else if (_threeGram[2] > other._threeGram[2]) {
            return 1;
        } else {
            return 0;
        }
    }
    
    @Override
    public String toString() {
        return new String(_threeGram);
    }

}
