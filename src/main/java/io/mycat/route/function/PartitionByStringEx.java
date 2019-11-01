package io.mycat.route.function;


/**
 * @author <a href="mailto:daasadmin@hp.com">yangwenx</a>
 */
public final class PartitionByStringEx extends PartitionByString {
    @Override
    public Integer calculate(String key) {
        if(key == null ) return 0;

        String keyEx = translate(key);
        System.out.println("the key is "+ keyEx);
        return super.calculate(keyEx);
    }
    public String translate(String key){
        String keyEx = key;
        if(key.contains(":")){
          keyEx = key.replaceAll(":", "");
        }else if(key.contains("-")){
            keyEx = key.replaceAll("-", "");
          }
        return keyEx;
    }
    
    public static void main(String[] args) {
        String mac1 = "90:f0:52:7e:61:8e";
        String mac2 = "00-34-cb-90-ef-98";
        
        String indexcode1 = "7cc0adb2-a3c3-48fd-b432-718103e85c28";
        String indexcode2 = "7cc0adb2-a3c3-48fd-b432-718103e85c27";
        System.out.println("ddddddd");
        PartitionByString rule = new PartitionByStringEx();
        String idVal = null;
        rule.setPartitionLength("512");
        rule.setPartitionCount("2");
        rule.init();
        rule.setHashSlice("0:2");
        // last 4
        rule = new PartitionByStringEx();
        rule.setPartitionLength("512");
        rule.setPartitionCount("2");
        rule.init();
        // last 4 characters
        // rule.setHashSlice("-4:0");
        idVal = "aaaabbb0000";
        int result = rule.calculate(mac1);
        System.out.println(result);
        result = rule.calculate(mac2);
        System.out.println(result);
        result = rule.calculate(indexcode1);
        System.out.println(result);
        result = rule.calculate(indexcode2);
        System.out.println(result);
    }
}