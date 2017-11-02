package tools;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;

public class AnsjTokenizer {
	public static void main(String[] args){
		String n = "交通银行股份有限公司无锡分行、江阴市展达化工贸易有限公司、、成都五牛运输有限公司、、、、、葛文峻、胡越男金融借款合同纠纷";
		for(Term t: NlpAnalysis.parse(n)){
			System.out.println(t.getName() + " " + t.getOffe());
		}
	}	
}
