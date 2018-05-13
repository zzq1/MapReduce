package com.neu.hive.UDAF;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@SuppressWarnings("deprecation")
public class Maximum extends UDAF {
  public static class MaximumDoubleUDAFEvaluator implements UDAFEvaluator{
    private Double result;

    public void init(){
      result = null;
    }

    public boolean iterate(Double value){
      if (value == null){
        return true;
      }

      if ( result == null){
        result = new Double(value);
      }else {
        result = Math.max(result, value);
      }

      return true;
    }

    public Double terminatePartiaal(){
      return result;
    }

    public boolean merge(Double other){
      return iterate(other);
    }

    public Double terminate(){
      return result;
    }
  }
}