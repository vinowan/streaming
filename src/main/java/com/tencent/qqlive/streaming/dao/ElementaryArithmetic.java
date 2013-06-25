package com.tencent.qqlive.streaming.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class ElementaryArithmetic {
	static class Operator implements Comparable<Operator> {
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
				return RIGHT_BRACKES;
			} else if (oper.equals("+")) {
				return ADD_OPERATOR;
			} else if (oper.equals("-")) {
				return SUB_OPERATOR;
			} else if (oper.equals("*")) {
				return MUL_OPERATOR;
			} else if (oper.equals("/")) {
				return DIV_OPERATOR;
			} else if (oper.equals("(")) {
				return LEFT_BRACKES;
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
	
	private static final Operator LEFT_BRACKES = new Operator('(', 0);
	private static final Operator RIGHT_BRACKES = new Operator(')', 0);
	private static final Operator ADD_OPERATOR = new Operator('+', 1);
	private static final Operator SUB_OPERATOR = new Operator('-', 1);
	private static final Operator MUL_OPERATOR = new Operator('*', 2);
	private static final Operator DIV_OPERATOR = new Operator('/', 2);
	
	
	private List<String> infixNotation = null;
	private List<String> postfixNotation = null;
		
	public ElementaryArithmetic(String expression) {
		infixNotation = tokenizer(expression);
		postfixNotation = toPostfix(infixNotation);
	}
	
	public double calcResult(Map<String, Double> values) {
		Stack<Double> resultStack = new Stack<Double>();
		
		Iterator<String> it = postfixNotation.iterator();
		while(it.hasNext()) {
			String notation = it.next();
			Operator oper = Operator.valueOf(notation);
			if (oper == null) {
				Double val = values.get(notation);
				if (val == null) {
					resultStack.push(0.0);
				} else {
					resultStack.push(val);
				}
			} else {
				double rhs = resultStack.pop();
				double lhs = resultStack.pop();
				
				resultStack.push(oper.calc(lhs, rhs));
			}
		}
		
		return resultStack.pop();
	}
	
	public String getInfixNotation() {
		StringBuilder sb = new StringBuilder();
		for (String notation : infixNotation) {
			sb.append(notation);
		}
		
		return sb.toString();
	}
	
	public String getPostfixNotation() {
		StringBuilder sb = new StringBuilder();
		for (String notation : postfixNotation) {
			sb.append(notation);
		}
		
		return sb.toString();
	}
	
	private List<String> tokenizer(String expression) {
		List<String> ret = new ArrayList<String>();
		
		StringBuilder token = new StringBuilder();
		
		char[] charArray = expression.toCharArray();
		for(int i = 0; i<charArray.length; i++) {
			if (Character.isWhitespace(charArray[i]))
				continue;
			
			if (Operator.isOperator(charArray[i])) {
				String value = token.toString();
				if (!value.equals("")) {
					ret.add(value);
					token = new StringBuilder();
				}
				ret.add(String.valueOf(charArray[i]));
			} else {
				token.append(charArray[i]);
			}
		}
		
		String value = token.toString();
		if (!value.equals("")) {
			ret.add(value);
		}
		
		return ret;
	}
	
	private List<String> toPostfix(List<String> infixNotation) {
		List<String> ret = new ArrayList<String>();
		Stack<Operator> opStack = new Stack<Operator>();
		
		for (String token : infixNotation) {
			Operator oper = Operator.valueOf(token);
			if (oper == null) {
				ret.add(token);
			} else {
				if (oper.equals(LEFT_BRACKES)) {
					opStack.push(oper);
				} else if (oper.equals(RIGHT_BRACKES)) {
					while(!opStack.peek().equals(LEFT_BRACKES)) {
						ret.add(opStack.pop().toString());
					}
					
					opStack.pop();
				} else {
					while(!opStack.empty() && oper.compareTo(opStack.peek()) <= 0) {
						ret.add(opStack.pop().toString());
					}
					
					opStack.push(oper);
				}
			}
		}
		
		while(!opStack.empty()) {
			ret.add(opStack.pop().toString());
		}
		
		return ret;
	}
	
	public static void main(String[] args) {
		ElementaryArithmetic ea = new ElementaryArithmetic("(hspeed + uspeed) / [hspeed]");
		
		Map<String, Double> values = new HashMap<String, Double>();	
		values.put("hspeed", 1.0);
		values.put("uspeed", 2.0);
		values.put("[hspeed]", 1.5);
		
		System.out.println(ea.calcResult(values));
	}
}
