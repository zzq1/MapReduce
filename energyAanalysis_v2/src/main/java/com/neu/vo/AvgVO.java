package com.neu.vo;

import java.io.Serializable;

public class AvgVO implements Serializable{

	private static final long serialVersionUID = 4293490247262330707L;

	private double sum;
	
	private int num;

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}
	
	public void addSum(double sum) {
		this.sum = this.sum + sum;
	}
	
	public void addNum(){
		this.num++;
	}
	
	public void addNum(int num){
		this.num = this.num + num;
	}
	
	public double getAvg(){
		if(this.num != 0) {
			return this.sum/this.num;
		}else {
			return 0;
		}
	}
	
}
