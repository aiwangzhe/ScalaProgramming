package spark;

import java.io.*;

public class ProcessMnistData {

    public static void main(String[] args) throws IOException {
        try {
            InputStream reader = new FileInputStream("src/main/resources/train-images.idx3-ubyte");
            BufferedInputStream br = new BufferedInputStream(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言

            InputStream labelStream = new FileInputStream("src/main/resources/train-labels.idx1-ubyte");
            byte[] header = new byte[8];
            byte[] imgHeader = new byte[16];
            labelStream.read(header);
            br.read(imgHeader);
            byte[] buffer = new byte[784];
            byte[] label = new byte[1];

            FileWriter writer = new FileWriter("train-data");

            while ((br.read(buffer)) != -1) {
                System.out.println("length: " + buffer.length);
                labelStream.read(label);

                writer.write(String.valueOf(Byte.toUnsignedInt(label[0])));
                writer.write(" ");
                for(int i = 0; i < buffer.length; i++){
                    int p = Byte.toUnsignedInt(buffer[i]);

                    writer.write((i +1) + ":" + (p / 255.0));
                    writer.write(" ");
                }
                writer.write("\n");
            }

            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
