class E2EApp {

    public static void main(String[] paramArrayOfString) {
        while (true) {
            primitiveLocalVariables();

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ex) {
            }
        }
    }

    public static void primitiveLocalVariables() {
        int i = 1;
        char c = 'h';
        String s = "hi";
        float f = 1.0f;
        long max = Long.MAX_VALUE;
        byte b = -2;
        short sh = Short.MIN_VALUE;
        double d = 00.23d;
        boolean bool = true;
        System.out.println(
                String.valueOf(i) + " " +
                        String.valueOf(c) + " " +
                        String.valueOf(s) + " " +
                        String.valueOf(f) + " " +
                        String.valueOf(max) + " " +
                        String.valueOf(b) + " " +
                        String.valueOf(sh) + " " +
                        String.valueOf(d) + " " +
                        String.valueOf(bool)
        );
    }
}
