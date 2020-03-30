
import java.util.HashMap;
import java.util.Map;


/**
 * @author dingchuangshi
 */
public class Solution {

    public int[] twoSum(int[] nums, int target) {
        int[] result = new int[2];
        Map<Integer,Integer> map = new HashMap<Integer,Integer>();
        // 利用hashMap
        for(int i = 0;i < nums.length;i++){
            map.put(nums[i],i);
        }

        // 
        for(int i = 0;i < nums.length;i++){
            int num = target - nums[i];
            if(map.containsKey(num) && map.get(num) != i){
                result[0] = i;
                result[1] = map.get(num);
                break;
            }
        }
        return result;

    }
}