package com.neu.hive.UDAF.mean;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.DoubleWritable;
@SuppressWarnings("deprecation")
public class Mean extends UDAF {
  public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator{

    public static class PartialResult{
      double sum;
      long count;
    }

    private PartialResult result;

    public void init(){
      result = null;
    }

    public boolean iterate(DoubleWritable value){
      if (value == null){
        return true;
      }

      if ( result == null){
        result = new PartialResult();
      }

      result.sum += value.get();
      result.count++;
      return true;
    }

    public PartialResult terminatePartiaal(){
      return result;
    }

    public boolean merge(DoubleWritable other){
      if ( other == null){
        return true;
      }

      if ( result == null){
        result = new PartialResult();
      }

      result.sum += result.sum;
      result.count += result.count;
      return true;
    }

    public DoubleWritable terminate(){
      if ( result == null){
        return null;
      }
      return new DoubleWritable( result.sum / result.count);
    }

  }

}