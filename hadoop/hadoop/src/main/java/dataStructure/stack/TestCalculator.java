package dataStructure.stack;

import java.util.Stack;

public class TestCalculator {

    /**
     * 1+2-3*4+5-6/2+3
     * @param args
     */
    public static void main(String[] args) throws Exception {
        CalculatorUtil calculatorUtil = new CalculatorUtil();
        calculatorUtil
                .addNumber(1).addCalculator(CalculatorUtil.CalculatorEnum.ADD)
                .addNumber(2).addCalculator(CalculatorUtil.CalculatorEnum.SUB)
                .addNumber(3).addCalculator(CalculatorUtil.CalculatorEnum.MUL)
                .addNumber(4).addCalculator(CalculatorUtil.CalculatorEnum.ADD)
                .addNumber(5).addCalculator(CalculatorUtil.CalculatorEnum.SUB)
                .addNumber(6).addCalculator(CalculatorUtil.CalculatorEnum.DIVIDED)
                .addNumber(2).addCalculator(CalculatorUtil.CalculatorEnum.ADD)
                .addNumber(3);

        System.out.println(calculatorUtil.toString());

    }
}
