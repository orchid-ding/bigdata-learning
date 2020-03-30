package test;

public class Arrange {
	public static void main(String[] args) {
		char[] chs = {'a','b','c'};
		arrange(chs, 0, chs.length);
	}
	public static void arrange(char[] chs, int start, int len){
		if(start == len-1){
			for(int i=0; i<chs.length; ++i)
				System.out.print(chs[i]);
			System.out.println();
			return;
		}
		for(int i=start; i<len; i++){
			char temp = chs[start];
			chs[start] = chs[i];
			chs[i] = temp;
			arrange(chs, start+1, len);
			temp = chs[start];
			chs[start] = chs[i];
			chs[i] = temp;
		}
	}
}