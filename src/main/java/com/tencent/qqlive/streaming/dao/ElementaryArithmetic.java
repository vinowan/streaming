package com.tencent.qqlive.streaming.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicReference;

public class ElementaryArithmetic {
	public static final Operator LEFT_BRACKES = new Operator('(', 0);
	public static final Operator RIGHT_BRACKES = new Operator(')', 0);
	public static final Operator ADD_OPERATOR = new Operator('+', 1);
	public static final Operator SUB_OPERATOR = new Operator('-', 1);
	public static final Operator MUL_OPERATOR = new Operator('*', 2);
	public static final Operator DIV_OPERATOR = new Operator('/', 2);
	
	private String itemName = null;
	
	private List<String> infixNotation = null;
	private List<String> postfixNotation = null;
	
	// token -> Operand, 函数compute和calcResult被不同的线程调用
	private AtomicReference<HashMap<String, Operand>> operandsRef = new AtomicReference<HashMap<String, Operand>>();
	
	private int count = 0;
		
	public ElementaryArithmetic(String itemName) {
		this.itemName = itemName;
		
		infixNotation = tokenizer(itemName);
		postfixNotation = toPostfix(infixNotation);
		
		resetOperands();
	}
	
	public String getItemName() {
		return itemName;
	}
	
	public List<String> getInfixNotation() {
		return infixNotation;
	}

	public List<String> getPostfixNotation() {
		return postfixNotation;
	}

	public int getCount() {
		return count;
	}

	public void compute(Map<String, String> itemValues, Map<String, ItemRange> itemRanges) {
		for (Map.Entry<String, Operand> entry : operandsRef.get().entrySet()) {
			String name = entry.getKey();
			if (name.startsWith("[") && name.endsWith("]"))
				name = name.substring(1, name.length() - 1);
			
			String value = itemValues.get(name);
			if (value == null)
				continue;
			
			// 表达式中的值都认为是Double类型
			double dVal = Double.valueOf(value);
			ItemRange range = itemRanges.get(name);
			if (range != null && !range.validate(dVal))
				continue;
			
			entry.getValue().compute(dVal);
//			System.out.println("******Operand:" + entry.getValue().getName() + "|" + entry.getValue().getValue());
		}
		
	}
	
	public double calcResult() {
		HashMap<String, Operand> operands = resetOperands();
		
		Stack<Double> resultStack = new Stack<Double>();
		
		Iterator<String> it = postfixNotation.iterator();
		while(it.hasNext()) {
			String notation = it.next();
			Operator oper = Operator.valueOf(notation);
			if (oper == null) {
				Operand val = operands.get(notation);
				if (val == null) {
					resultStack.push(0.0);
				} else {
					resultStack.push(val.getValue());
					if (count < val.getCount())
						count = val.getCount();
				}
			} else {
				double rhs = resultStack.pop();
				double lhs = resultStack.pop();
				
				resultStack.push(oper.calc(lhs, rhs));
			}
		}
		
		return resultStack.pop();
	}
	
	public Set<String> getExpression() {
		Set<String> result = new HashSet<String>();
		
		for (String token : infixNotation) {
			Operator oper = Operator.valueOf(token);
			if (oper == null) {
				if (token.startsWith("[") && token.endsWith("]")) {
					token = token.substring(1, token.length() - 1);
				}
				
				result.add(token);
			}
		}
		
		return result;
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
	
	private HashMap<String, Operand> resetOperands() {
		HashMap<String, Operand> operands = new HashMap<String, Operand>();
		for (String token : infixNotation) {
			Operator oper = Operator.valueOf(token);
			if (oper == null) {
				operands.put(token, new Operand(token));
			}
		}
		
		return operandsRef.getAndSet(operands);
	}
	
	public static void main(String[] args) {
		Map<String, ItemRange> itemRanges = new HashMap<String, ItemRange>();
		ItemRange hspeedRange = new ItemRange();
		hspeedRange.setItem("hspeed");
		hspeedRange.setMin(1);
		hspeedRange.setMax(3);
		itemRanges.put("hspeed", hspeedRange);
		
		ItemRange uspeedRange = new ItemRange();
		uspeedRange.setItem("uspeed");
		uspeedRange.setMin(2);
		uspeedRange.setMax(4);
		itemRanges.put("uspeed", uspeedRange);
		
		ElementaryArithmetic ea = new ElementaryArithmetic("(hspeed + uspeed) / [hspeed]");
		
		Map<String, String> itemValues = new HashMap<String, String>();
		itemValues.put("hspeed", "2");
		itemValues.put("uspeed", "3");
		ea.compute(itemValues, itemRanges);
		
		itemValues.clear();
		itemValues.put("hspeed", "3");
		itemValues.put("uspeed", "5");
		ea.compute(itemValues, itemRanges);
		
		System.out.println(ea.calcResult());
		System.out.println(ea.getCount());
		
//		itemValues.clear();
//		itemValues.put("hspeed", "3");
//		itemValues.put("uspeed", "2");
//		ea.compute(itemValues, itemRanges);
//		System.out.println(ea.calcResult());
	}
}
