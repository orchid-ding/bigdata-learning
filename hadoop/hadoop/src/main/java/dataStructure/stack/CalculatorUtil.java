package dataStructure.stack;

import org.apache.flink.table.expressions.In;

import java.util.Stack;

/**
 * 使用自定义栈，实现四则运算
 * @author dingchuangshi
 */
public class CalculatorUtil {

    private static MyStack numberStack = new MyStack<String>(20);

    private static MyStack calculatorStack = new MyStack<CalculatorEnum>(20);

    private boolean pushNumber = true;

    public CalculatorUtil addNumber(int number) throws Exception {
        if(!pushNumber){
           throw new Exception("please add calculator");
        }
        if(numberStack.size() > 0){
            CalculatorEnum calculator = (CalculatorEnum) calculatorStack.pre();
            if(CalculatorEnum.DIVIDED == calculator || CalculatorEnum.MUL == calculator){
                Integer num = (Integer) numberStack.pop();
                CalculatorEnum cal = (CalculatorEnum) calculatorStack.pop();
                if(cal == CalculatorEnum.DIVIDED){
                    number = num / number;
                }else if(cal == CalculatorEnum.MUL){
                    number = num * number;
                }
            }
        }

        numberStack.push(number);
        pushNumber = !pushNumber;
        return this;
    }

    public CalculatorUtil addCalculator(CalculatorEnum calculator) throws Exception {
        if(pushNumber){
            throw new Exception("please add number");
        }
        calculatorStack.push(calculator);
        pushNumber = !pushNumber;
        return this;
    }

    public MyStack getCalculatorStack() {
        return calculatorStack;
    }

    public MyStack getNumberStack() {
        return numberStack;
    }

    @Override
    public String toString() {
        int sum = 0;
        int numberSize = numberStack.size();
        StringBuffer sb = new StringBuffer(numberStack.size() * 2);
        for (int i = 0; i <numberSize ; i++) {
            int number = (int) numberStack.pop();
            sb.append( number+ " ");
            if(calculatorStack.size() > 0){
                CalculatorEnum calculatorEnum = (CalculatorEnum) calculatorStack.pop();
                if(CalculatorEnum.ADD == calculatorEnum){
                    sum += number;
                }else if(CalculatorEnum.SUB == calculatorEnum){
                    sum -= number;
                }
                sb.append(calculatorEnum.getFlag());
            }else {
                sum += number;
            }
        }
        sb.append( "= " + sum);
        return sb.toString();
    }

    public enum CalculatorEnum {

        ADD(" + "),
        DIVIDED(" / "),
        MUL(" * "),
        SUB(" - ");

        private String flag;

        CalculatorEnum(String flag){
            this.flag = flag;
        }

        public String getFlag(){
            return flag;
        }

        public void setFlag(String flag){
            this.flag = flag;
        }




    }

}
