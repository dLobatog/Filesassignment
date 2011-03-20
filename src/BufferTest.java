/**
 *Ejemplo de uso de la memoria intermedia. 
 *Se utiliza una memoria intermedia con pol�tica de liberaci�n aleatoria.
 */

import fileSystem.utils.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BufferTest {
        
    /**
	 * Ejemplo de uso de una memoria intermedia con pol�tica de liberaci�n aleatoria de 'numberOfPages' p�ginas de tama�o 'blockSize' bytes.
	 * @param args
	 */
	public static void main(String[] args) {
            int numberOfPages=2; //N�mero de p�ginas de la memoria intermedia que se crear�
            int blockSize=16; //Tama�o de las p�ginas de la memoria intermedia

            //Se crea una memoria intermedia de n�mero y tama�o de p�ginas definido. 
            //La memoria intermedia permite trabajar con bloques de uno o m�s ficheros.
            //Cada p�gina de memoria podr� alojar el contenido de alguno de los bloques de los ficheros abiertos.
		 
		Buffer  buffer = new RABuffer(numberOfPages,blockSize); 
		ByteBuffer block; // Referencia a un bloque de memoria (java.nio.ByteBuffer) 
		FileChannel f0;    // Referencia a un fichero (java.nio.channel.FileChannel)
        FileChannel f1;    // Referencia a otro fichero (java.nio.channel.FileChannel)
		
		String f0String0 = "f0 string 0";//Cadena de caracteres que se escribir� en el fichero f0 en el bloque 0
		String f0String1 = "f0 string 1";//Cadena de caracteres que se escribir� en el fichero f0 en el bloque 1
        String f0String2 = "f0 string 2";//Cadena de caracteres que se escribir� en el fichero f0 en el bloque 2
                
        String f1String0 = "f1 string 0";//Cadena de caracteres que se escribir� en el fichero f1 en el bloque 0
		String f1String1 = "f1 string 1";//Cadena de caracteres que se escribir� en el fichero f1 en el bloque 1
        String f1String2 = "f1 string 2";//Cadena de caracteres que se escribir� en el fichero f1 en el bloque 2
                
        byte [] bytesOfString=new byte[11]; //Array que se utilizar� para recuperar en forma de bytes las cadenas de caracteres de los ficheros
                
		try {
                        //Estado inicial de las p�ginas de la memoria intermedia
                        System.out.println("\nInitial state\n");
                        buffer.print();
                    
                        System.out.println("\n1: Write block 0 (testFile0.txt)\n");
                        
			// Se abre el fichero 'prueba0.txt' en la memoria intermedia en la variable f0 (si no existe el fichero se crea)
			f0 = buffer.openFile("testFile0.txt","rw");	
                        
                        // Se adquiere el primer bloque del fichero f0 a trav�s de la memoria intermedia (1 acceso)
			block = buffer.acquireBlock(f0, 0);
                                                	
			// Se lleva el puntero a la posici�n inicial del bloque
			block.clear();
                        //Se escribe en el bloque la cadena de caracteres f0String0.
			block.put(f0String0.getBytes());

                        //Estado actual de las p�ginas de la memoria intermedia
                        buffer.print();
                        
                        System.out.println("\n2: Write block 1 (testFile0.txt)\n");    
                        
                        // Se adquiere el bloque 1 de f0, se lleva el puntero a la posici�n inicial y se escribe en �l la cadena de caracteres f0String1 (1 acceso, van 2)
                        block = buffer.acquireBlock(f0, 1);
			block.clear();
			block.put(f0String1.getBytes());
                        
                        //Estado actual de las p�ginas de la memoria intermedia
                        buffer.print();
                        
                        System.out.println("\n3: Write block 2 (testFile0.txt)\n");
                        
                        // Se adquiere el bloque 2 de f0, se lleva el puntero a la posici�n inicial y se escribe en �l la cadena de caracteres f0String2 (1 acceso, van 3)
                        block = buffer.acquireBlock(f0, 2);
			block.clear();
			block.put(f0String2.getBytes());
                        
                        //Estado actual de las p�ginas de la memoria intermedia
                        buffer.print();
                        
                        System.out.println("\n4: Read block 0 (testFile0.txt) and replace it by capital letters\n");
                        
                        // Se recupera el contenido del bloque 0 de f0, se lleva el puntero a la posici�n inicial y se imprimen por pantalla sus 11 primeros bytes (m�ximo 1 acceso si el bloque ya no estuviera cargado en alguna p�gina, como mucho van 4)
                        block = buffer.acquireBlock(f0, 0);
                        block.clear();                        
                        block.get(bytesOfString);
                        System.out.println(new String(bytesOfString));
                        //Se pone lleva el puntero a la posici�n inicial del block y se sustituye por la misma cadena en may�sculas
                        block.clear();
                        block.put(new String(bytesOfString).toUpperCase().getBytes());
			                       
                        //Estado actual de las p�ginas de la memoria intermedia
                        buffer.print();
                        
                        System.out.println("\n5: Write block 0 (testFile1.txt)\n");
                        
                        // Se abre el fichero 'prueba1.txt' en la memoria intermedia en la variable f1 (si no existe el fichero se crea).
                        f1 = buffer.openFile("testFile1.txt","rw");
                        
                        // Se adquiere el bloque 0 de f1, se lleva el puntero a la posici�n inicial y se escribe en �l la cadena de caracteres f1String0 (1 acceso, como mucho van 5)
                        block = buffer.acquireBlock(f1, 0);
			block.clear();
			block.put(f1String0.getBytes());
                        
                        //Estado actual de las p�ginas de la memoria intermedia
                        buffer.print();
                        
                        System.out.println("\n6: Write block 1 (testFile1.txt)\n");
                        
                        // Se adquiere el bloque 1 de f1, se lleva el puntero a la posici�n inicial y se escribe en �l la cadena de caracteres f1String1 (1 acceso, como mucho van 6)
                        block = buffer.acquireBlock(f1, 1);
			block.clear();
			block.put(f1String1.getBytes());
                        
                        //Estado actual de las p�ginas de la memoria intermedia
                        buffer.print();
                        
                        System.out.println("\n7: Write block 2 (testFile1.txt)\n");
                        
                        // Se adquiere el bloque 2 de f1, se lleva el puntero a la posici�n inicialy se escribe en �l la cadena de caracteres f1String2 (1 acceso, como mucho van 7)
                        block = buffer.acquireBlock(f1, 2);
			block.clear();
			block.put(f1String2.getBytes());
                        
                        //Estado actual de las p�ginas de la memoria intermedia
                        buffer.print();
                        
                        //Se recuperan todas las cadenas de caracteres escritas en los ficheros sabiendo que todas tienen 11 bytes.
                                                                      
                        block = buffer.acquireBlock(f0, 0); //(1 acceso m�ximo si este bloque ya ha sido liberado con antelaci�n, como mucho van 8)
                        block.clear();                        
                        block.get(bytesOfString); //Lectura de toda la cadena
                        System.out.println("\n8: Read block 0 (testFile0.txt):'"+new String(bytesOfString)+"'\n");

                        block = buffer.acquireBlock(f0, 1); //(1 acceso m�ximo si este bloque ya ha sido liberado con antelaci�n, como mucho van 9)
                        block.clear();
                        byte[] firstBytesOfString=new byte[5];
                        byte[] lastBytesOfString=new byte[6];
                        block.get(firstBytesOfString); //Lectura de media cadena
                        block.get(lastBytesOfString); //Lectura de la otra media cadena
                        System.out.println("\n9 Read block 1 (testFile0.txt):'"+new String(firstBytesOfString)+new String(lastBytesOfString)+"'\n");
                        
                        block = buffer.acquireBlock(f0, 2); //(1 acceso m�ximo si este bloque ya ha sido liberado con antelaci�n, como mucho van 10)
                        block.clear();                        
                        for(int i=0;i<11;i++)
                            bytesOfString[i]=block.get(); //Lectura byte a byte
                        System.out.println("\n10 Read block 2 (testFile0.txt):'"+new String(bytesOfString)+"'\n");
                        block = buffer.acquireBlock(f1, 0); //(1 acceso m�ximo si este bloque ya ha sido liberado con antelaci�n, como mucho van 11)
                        block.clear();
                        block.get(bytesOfString);
                        System.out.println("\n11 Read block 0 (testFile1.txt):'"+new String(bytesOfString)+"'\n");
                        
                        block = buffer.acquireBlock(f1, 1); //(1 acceso m�ximo si este bloque ya ha sido liberado con antelaci�n, como mucho van 12)
                        block.clear();
                        firstBytesOfString=new byte[5];
                        lastBytesOfString=new byte[6];
                        block.get(firstBytesOfString); //Lectura de media cadena
                        block.get(lastBytesOfString); //Lectura de la otra media cadena
                        System.out.println("\n12 Read block 1 (testFile1.txt):'"+new String(firstBytesOfString)+new String(lastBytesOfString)+"'\n");
                        
                        block = buffer.acquireBlock(f1, 2); //(1 acceso m�ximo si este bloque ya ha sido liberado con antelaci�n, como mucho van 13)
                        block.clear();
                        for(int i=0;i<11;i++)
                            bytesOfString[i]=block.get(); //Lectura byte a byte
                        System.out.println("\n13 Read block 2 (testFile1.txt):'"+new String(bytesOfString)+"'\n");
                                                                  
			// Se cierran los ficheros
			buffer.close(f0);
                        buffer.close(f1);
                        
			// N�mero de accesos realizados a memoria secundaria. A medida que se aumenta el n�mero de p�ginas de la memoria intermedia se reduce el n�mero de accesos --> probar ;)			
			System.out.println("\n\n"+buffer.getNumberOfAccesses()+" accesses to secondary memory < 13 blocks read or writed");
									
			
		} catch (IOException e) {
			// Si se producen problemas, mostramos el origen del problema 
			e.printStackTrace();
		}
			
	}        
           
}
