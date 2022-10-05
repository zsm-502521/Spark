public class test11 {
    public static void main(String[] args) {
        System.out.println("小星 ");

        for (float y = 1.5f; y > -1.5f; y -= 0.15f) {
            for (float x = -1.5f; x < 1.5f; x += 0.05) {
                float a = x * x + y * y - 1;
                if ((a * a * a - x * x * y * y * y) <= 0.0) {
                    System.out.print("\033[31m" +"*");
                } else {
                    System.out.print("\033[30m" +" ");
                }
            }
            System.out.println();
        }

        System.out.println(" \033[(前缀),m(后缀),格式:\033[XX;XX;XXm");
        System.out.println("------ 字体颜色30~37 ------");
        System.out.println("\033[30m" + "就是酱紫的");
        System.out.println("\033[31m" + "就是酱紫的");
        System.out.println("\033[32m" + "就是酱紫的");
        System.out.println("\033[37m" + "就是酱紫的");
        System.out.println("------ 背景颜色40~47 -------");
        System.out.println("\033[43m" + "就是酱紫的");
        System.out.println("\033[44m" + "就是酱紫的"+"\033[m");
        System.out.println("\033[45m" + "就是酱紫的");
        System.out.println("\033[46m" + "就是酱紫的"+"\033[m");
        System.out.println("--- 1:加粗,;:隔开,90~97字体颜色变亮 ---");
        System.out.println("\033[1;90m" + "就是酱紫的");
        System.out.println("\033[1;94m" + "就是酱紫的");
        System.out.println("\033[1;95m" + "就是酱紫的");
        System.out.println("\033[1;97m" + "就是酱紫的");
        System.out.println("\033[1;93;45m" + "就是酱紫的"+"\033[m");



    }
}
