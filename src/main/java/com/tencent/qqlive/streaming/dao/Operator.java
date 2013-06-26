package com.tencent.qqlive.streaming.dao;

class Operator implements Comparable<Operator> {
	private final char token;
	private final int  priority;
	
	public Operator(char token, int priority) {
		this.token = token;
		this.priority = priority;
	}
	
	public double calc(double lhs, double rhs) {
		double ret = 0;
		
		switch (token) {
		case '+':
			ret = lhs + rhs;
			break;
		case '-':
			ret = lhs - rhs;
			break;
		case '*':
			ret = lhs * rhs;
			break;
		case '/':
			ret = lhs / rhs;
			break;
		case ')':
		case '(':
		default:
			ret = 0;
		}
		
		return ret;
	}
	
	public int compareTo(Operator o) {
		return priority - o.priority;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Operator) {
			Operator oper = (Operator)obj;
			return token == oper.token;
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		return String.valueOf(token);
	}
	
	public static Operator valueOf(String oper) {
		if (oper.equals(")")) {
			return ElementaryArithmetic.RIGHT_BRACKES;
		} else if (oper.equals("+")) {
			return ElementaryArithmetic.ADD_OPERATOR;
		} else if (oper.equals("-")) {
			return ElementaryArithmetic.SUB_OPERATOR;
		} else if (oper.equals("*")) {
			return ElementaryArithmetic.MUL_OPERATOR;
		} else if (oper.equals("/")) {
			return ElementaryArithmetic.DIV_OPERATOR;
		} else if (oper.equals("(")) {
			return ElementaryArithmetic.LEFT_BRACKES;
		} else {
			return null;
		}
	}
	
	public static boolean isOperator(char c) {
		switch (c) {
		case ')':
		case '+':
		case '-':
		case '*':
		case '/':
		case '(':
			return true;
		default:
			return false;
		}
	}
}