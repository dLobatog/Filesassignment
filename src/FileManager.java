import fileSystem.utils.AbstractFileManager;
import fileSystem.utils.LogicalRecord;
import fileSystem.utils.Buffer;
import fileSystem.utils.UserInterface;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Vector;

/**
 * <p>Clase que implementa los métodos que son invocados por la interfaz de usuario para la realizaci�n de las funciones de gesti�n de ficheros. Estas funciones contemplan:</p>
 * <lu>
 * <li>Apertura de un sistema de ficheros a partir del nombre de un fichero de referencia</li>
 * <li>Cierre del sistema de ficheros abierto</li>
 * <li>Volcado de la informaci�n almacenada en la memoria intermedia al fichero correspondiente</li>
 * <li>Importaci�n de los registros de un fichero seg�n el dise�o original a los nuevos ficheros dise�ados</li>
 * <li>Consulta de aquellos registros del sistema que cumplen una condici�n especificada</li>
 * <li>Acceso invertido a los �ndices del sistema</li>
 * </lu>
 * Para realizar dichas funciones, el alumno deber� implementar cada uno de los m�todos abstractos de esta clase.
 * </p>
 * <p>S�lo ser� v�lido el acceso a los ficheros a trav�s de la <code>Buffer</code> que implementa este paquete.</p>
 */

public class FileManager extends AbstractFileManager{
    
    //Memoria intermedia que se utilizar� para acceder a los bloques de los ficheros.
    private Buffer buffer=null;
    FileChannel fc=null;
    ByteBuffer block = null;
    ByteArray myByteArray = new ByteArray();
    int currentblock = 0;
    int currentBlockInOverflowArea = 668;
    int[] alreadyWritten = new int[667];
    int alreadyWrittenOverflow = 0;
    int[] hashArray = new int[667];
    boolean[] resultSet = new boolean[667]; 
    int hashCounter = 0;
    int recordSize = 0;
    short lengthShort;
    short yearShort;
    // Length and existence marks
    byte titleLength, nationalityLength, voLength,
    nameLength, surnameLength, nicknameLength, nicknameExist;
    // Arrays where the fields will be saved according to the physical logical design.
    byte [] topicExist = new byte[16];
    byte [] title = new byte[titleLength]; 
    byte [] nationality = new byte[nationalityLength];
    byte [] vo = new byte[voLength];
    byte [] year;
    // Topic may be designed as a linked list or as a bidimensional array
    byte [] topic = new byte[16];
    // A short in java is 2 bytes so length could be of this type
    byte [] length;
    // An int in java is 4 bytes so takings could be of this type
    byte [] takings;
    byte [] directorName;
    byte directorNameLength;
    byte [] directorSurname;
    byte directorSurnameLength;
    byte [] directorNickname;
    byte directorNicknameLength;
    byte [] screenwriterName;
    byte screenwriterNameLength;
    byte [] screenwriterSurname;
    byte screenwriterSurnameLength;
    byte [] screenwriterNickname;
    byte screenwriterNicknameLength;
    byte [][] actorName = new byte [8][];
    byte [] actorNameLength = new byte [8];
    byte [][] actorSurname = new byte [8][];
    byte [] actorSurnameLength = new byte [8];
    byte [][] actorNickname = new byte [8][];
    byte [] actorNicknameLength = new byte [8];
    byte [] actorExist = new byte [8];
    byte [] ppl = new byte[2];
    byte overflow;
    
    public FileManager() {
        //Construye una memoria intermedia con pol�tica de liberaci�n aleatoria de 16 p�ginas de 1024 bytes.
        buffer=new RABuffer(); 
    }

    /**
    * Abre el sistema de ficheros a partir del nombre del fichero especificado como par�metro.
    * 
    * @param fileName nombre del fichero a abrir.
    * @return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */    
    public String openFileSystem(String fileName) {
    	//Open file fileName with all permissions allowed a
    	try {
			fc = buffer.openFile(fileName, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		
		return "File system ("+fileName+")' is now open";
    }

    /**
    *Cierra el sistema de ficheros. Previamente vuelca los datos de la memoria intermedia y cierra los ficheros abiertos. 
    *@return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */
    public String closeFileSystem() {      
    	//Set the policy to release all the pages in the block to intermediate memory.
    	if (fc != null){
    		buffer.releasePagePolicy(fc, buffer.getNumberOfPages());
    		buffer.close(fc);
    		return "File system is now closed";
    	}
    	else{
    		return "Closing";
    	}
    }

    /**
    * Fuerza la escritura del contenido de la memoria intermedia en los ficheros correspondientes.  
    *@return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */  
    public String flush() {
    	//Save the buffer in the file
    	buffer.save(fc);
        return "Files are saved";
    }

    /**
    * Lee un fichero de organizaci�n serial consecutiva y dise�o inicial de los registros y almacena su contenido 
    * en los nuevos ficheros dise�ados.
    * 
    * @param fileName Nombre completo del archivo desde el que se importa
    */
    public String importFile(String fileName) {
    	//Auxiliary array
    	byte bytesOfString[];
    	String stringField;
    	FileChannel importfc = null;
    	boolean EOF = false;
    	int eofCounter = 0;
    	int currentBlock = 0;
    	int counter = 0;
    	int usefulCounter = 0;
    	int realCounter = 0;
        //Keep track of how many bytes are left in block
        int bytesRead = 0;
        int topicCounter = 0;
        boolean topicFound = false;
        
		try {
			importfc = buffer.openFile(fileName, "rw");
			block = buffer.acquireBlock(importfc, currentBlock);
			block.clear();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IllegalArgumentException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		while(!EOF){
			try {
				block = buffer.acquireBlock(importfc, currentBlock);
				block.position(bytesRead);
				//Get title and convert it to the new physical-logical design
				bytesOfString = new byte[70];
				//block.get(bytesOfString) 
				for(int j = 0 ; j < 70 ; j++){
					bytesOfString[j] = block.get();
					bytesRead++;
					if(bytesOfString[j] == 35){
						eofCounter++;
					}
					if(bytesRead == 1024){
							currentBlock++;
							//counter++;
							bytesRead=0;
							block = buffer.acquireBlock(importfc, currentBlock);
							block.clear();
							System.out.println(currentBlock + " file channel position (import fc): " + importfc.position());
					}
				}
				stringField = new String(bytesOfString);
		    	title = stringToByte(stringField);
				titleLength = (byte) title.length;
				System.out.println(new String(title));
				if (eofCounter >= 4){
					System.out.println("Useful bytes " + usefulCounter);
					System.out.println("AVG. useful bytes/record " + usefulCounter/counter);
					System.out.println("Real bytes "+ realCounter);
					System.out.println("AVG. real bytes/record " + realCounter/counter);
					System.out.println("Nr of records " + counter);
					System.out.println("EOF reached. Stopping..");
					EOF = true;
					/* only for debug purposes */ System.out.println("####");
				}
				else{
					counter++;
					recordSize = 0;
					usefulCounter = usefulCounter + title.length;
					realCounter = realCounter + title.length + 1;
					recordSize = recordSize + title.length + 1;
					//Get nationality and convert it to the new physical logical design
					bytesOfString = new byte[14];
					for(int j = 0; j < 14 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					nationality = stringToByte(stringField);
					nationalityLength = (byte) nationality.length;
					/* only for debug purposes */ System.out.println(new String(nationality));
					usefulCounter = usefulCounter + nationality.length;
					realCounter = realCounter + nationality.length + 1;
					recordSize = recordSize + nationality.length + 1;
					//Get vo and convert it to the new physical-logical design
					bytesOfString = new byte[12];
					for(int j = 0 ; j < 12 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								 
								 
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					vo = stringToByte(stringField);
					voLength = (byte) vo.length;
					/* only for debug purposes */ System.out.println(new String(vo));
					usefulCounter = usefulCounter + vo.length;
					realCounter = realCounter + vo.length + 1;
					recordSize = recordSize + vo.length + 1;
					//Get year and convert it to the new physical-logical design (it is required to know the
					//year of the first movie and the year of the last movie in the database)
					bytesOfString = new byte[4];
					for(int j = 0 ; j < 4 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					yearShort = Short.parseShort(stringField);
					//yearShort = readShort(bytesOfString, 0);
					year = shortToByte(yearShort);
					
					/* only for debug purposes */ System.out.println(new String(year));
					usefulCounter = usefulCounter + 2;
					realCounter = realCounter + 2;
					recordSize = recordSize + 2;
					//Get the topics. In our design a mark of existence will be written before the set of fields.
					//This mark will consist in a byte specifying how many topics there will be
					bytesOfString = new byte[15];
					topicCounter = 0;
					for(int k = 0; k < 16; k++){
						for(int j = 0 ; j < 15 ; j++){
							bytesOfString[j] = block.get();
							bytesRead++;
							if(bytesRead == 1024){
									currentBlock++;
									//counter++;
									bytesRead=0;
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						topicFound = false;
						if (stringField.contains("suspense")){
							topic[k] = (byte) 1;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("romantica")){
							topic[k] = (byte) 2;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("comedia")){
							topic[k] = (byte) 3;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("aventura")){
							topic[k] = (byte) 4;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("fantasia")){
							topic[k] = (byte) 5;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("infantil")){
							topic[k] = (byte) 6;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("western")){
							topic[k] = (byte) 7;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("policiaco")){
							topic[k] = (byte) 8;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("historica")){
							topic[k] = (byte) 9;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("drama")){
							topic[k] = (byte) 10;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("accion")){
							topic[k] = (byte) 11;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("belica")){
							topic[k] = (byte) 12;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("ciencia ficcion")){
							topic[k] = (byte) 13;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("terror")){
							topic[k] = (byte) 14;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("animacion")){
							topic[k] = (byte) 15;
							topicCounter++;
							topicFound = true;
						}
						if (stringField.contains("musical")){
							topic[k] = (byte) 16;
							topicCounter++;
							topicFound = true;
						}
						//This means that there exist a topic in position # k.
						if(topicFound){
							topicExist[k] = 1;
							realCounter = realCounter + 1;
							recordSize = recordSize + 1;
							/* only for debug purposes */ System.out.println(stringField);
						}
						else{
							topicExist[k]=0;
							realCounter = realCounter + 1;
							recordSize = recordSize + 1;
						}
					}
					usefulCounter = usefulCounter + topicCounter;
					realCounter = realCounter + topicCounter;
					recordSize = recordSize + topicCounter;
					//Get length of the movie and convert it to the new physical-logical design
					bytesOfString = new byte[3];
					for(int j = 0 ; j < 3 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0; 
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					/* only for debug purposes */
					//myByteArray.writeBytes(length, 0, 2);
					//length = shortToByte(Short.parseShort(stringField));
					lengthShort = Short.parseShort(stringField);
					//lengthShort = readShort(bytesOfString,0);
					length = shortToByte(lengthShort);
					System.out.println(lengthShort);																				
					usefulCounter = usefulCounter + 2;
					realCounter = realCounter + 2;
					recordSize = recordSize + 2;
					//Get takings and convert it to the new physical-logical design 
					bytesOfString = new byte[9];
					for(int j = 0 ; j < 9 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					takings = ByteBuffer.allocate(4).putInt(Integer.parseInt(stringField)).array();
					//int takingsInteger = byteArrayToInt(bytesOfString);
					//takings = intToByteArray(takingsInteger);
					//takings = stringToByte(stringField);
					/* only for debug purposes */ System.out.println(new String(takings));
					usefulCounter = usefulCounter + 4;
					realCounter = realCounter + 4;
					recordSize = recordSize + 4;
					//Get director's name and convert it to the new physical-logical design
					bytesOfString = new byte[35];
					for(int j = 0 ; j < 35 ; j++){			
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					directorName = stringToByte(stringField);
					directorNameLength = (byte) directorName.length;
					/* only for debug purposes */ System.out.println(new String(directorName));
					usefulCounter = usefulCounter + directorName.length;
					realCounter = realCounter + directorName.length + 1;
					recordSize = recordSize + directorName.length + 1;
					//Get director's surname and convert it to the new physical-logical design
					bytesOfString = new byte[15];
					for(int j = 0 ; j < 15 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					directorSurname = stringToByte(stringField);
					directorSurnameLength = (byte) directorSurname.length;
					/* only for debug purposes */ System.out.println(new String(directorSurname));
					usefulCounter = usefulCounter + directorSurname.length;
					realCounter = realCounter + directorSurname.length + 1;
					recordSize = recordSize + directorSurname.length + 1;
					//Get director's nickname and convert it to the new physical-logical design
					bytesOfString = new byte[25];
					for(int j = 0 ; j < 25 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					directorNickname = stringToByte(stringField);
					directorNicknameLength = (byte) directorNickname.length;
					/* only for debug purposes */ System.out.println(new String(directorNickname));
					usefulCounter = usefulCounter + directorNickname.length;
					realCounter = realCounter + directorNickname.length + 1;
					recordSize = recordSize + directorNickname.length + 1;
					//Get screenwriters's name and convert it to the new physical-logical design
					bytesOfString = new byte[35];
					for(int j = 0 ; j < 35 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					screenwriterName = stringToByte(stringField);
					screenwriterNameLength = (byte) screenwriterName.length;
					/* only for debug purposes */ System.out.println(new String(screenwriterName));
					usefulCounter = usefulCounter + screenwriterName.length;
					realCounter = realCounter + screenwriterName.length + 1;
					recordSize = recordSize + screenwriterName.length + 1;
					//Get screenwriter's surname and convert it to the new physical-logical design
					bytesOfString = new byte[15];
					for(int j = 0 ; j < 15 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					screenwriterSurname = stringToByte(stringField);
					screenwriterSurnameLength = (byte) screenwriterSurname.length;
					/* only for debug purposes */ System.out.println(new String(screenwriterSurname));
					usefulCounter = usefulCounter + screenwriterSurname.length;
					realCounter = realCounter + screenwriterSurname.length + 1;
					recordSize = recordSize + screenwriterSurname.length + 1;
					//Get screenwriter's nickname and convert it to the new physical-logical design
					bytesOfString = new byte[25];
					for(int j = 0 ; j < 25 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								//counter++;
								bytesRead=0;
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					screenwriterNickname = stringToByte(stringField);
					screenwriterNicknameLength = (byte) screenwriterNickname.length;
					/* only for debug purposes */ System.out.println(new String(screenwriterNickname));
					usefulCounter = usefulCounter + screenwriterNickname.length;
					realCounter = realCounter + screenwriterNickname.length + 1;
					recordSize = recordSize + screenwriterNickname.length + 1;
					for(int k = 0 ; k < 8 ; k++){
						//Get actors's name and convert it to the new physical-logical design
						bytesOfString = new byte[35];
						for(int j = 0 ; j < 35 ; j++){
							bytesOfString[j] = block.get();
							bytesRead++;
							if(bytesRead == 1024){
									currentBlock++;
									//counter++;
									bytesRead=0;
									 
									 
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						actorName[k] = stringToByte(stringField);
						actorNameLength[k] = (byte) actorName[k].length;
						usefulCounter = usefulCounter + actorName[k].length;
						realCounter = realCounter + actorName[k].length + 1;
						recordSize = recordSize + actorName[k].length + 1;
						//Get actor's surname and convert it to the new physical-logical design
						bytesOfString = new byte[15];
						for(int j = 0 ; j < 15 ; j++){
							bytesOfString[j] = block.get();
							bytesRead++;
							if(bytesRead == 1024){
									currentBlock++;
									//counter++;
									bytesRead=0;
									 
									 
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						actorSurname[k] = stringToByte(stringField);
						actorSurnameLength[k] = (byte) actorSurname[k].length;
						usefulCounter = usefulCounter + actorSurname[k].length;
						realCounter = realCounter + actorSurname[k].length + 1;
						recordSize = recordSize + actorSurname[k].length + 1;
						//Get actor's nickname and convert it to the new physical-logical design
						bytesOfString = new byte[25];
						for(int j = 0 ; j < 25 ; j++){
							bytesOfString[j] = block.get();
							bytesRead++;
							if(bytesRead == 1024){
									currentBlock++;
									//counter++;
									bytesRead=0;
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						actorNickname[k] = stringToByte(stringField);
						actorNicknameLength[k] = (byte) actorNickname[k].length;
						usefulCounter = usefulCounter + actorNickname[k].length;
						realCounter = realCounter + actorNickname[k].length + 1;
						recordSize = recordSize + actorNickname[k].length + 1;
						//This means that there exist a topic in position # k.
						if(actorNicknameLength[k]!=0){
							actorExist[k] = 1;
							realCounter = realCounter + 1;
							recordSize = recordSize + 1;
							/* only for debug purposes */ System.out.println(new String(actorName[k]));
							/* only for debug purposes */ System.out.println(new String(actorSurname[k]));
							/* only for debug purposes */ System.out.println(new String(actorNickname[k]));
						}
						else{
							realCounter = realCounter + 1;
							recordSize = recordSize + 1;
							actorExist[k]=0;
						}
					}
					System.out.println("The record size is "+ recordSize);
				}
			// Write to file
			write();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
        return "Method 'FileManager.importFile("+fileName+")' not implemented";
    }
    
    /**
     * createHash
     * 
     * Creates a hash to be the equal to the key 
     * 
     */
    
    public int createHash(byte[] input){
    	int titleSum1 = 0;
    	int titleSum2 = 0;
		int key = 0;
    	
    	for (int i = 0 ; i <(input.length/2) ; i ++){
			titleSum1 += input[i];
		}
		for (int i = (input.length/2) ; i <input.length ; i ++){
			titleSum2 += input[i];
		}
		
		key = (titleSum1 + titleSum2) % 667;
    	return key;
    }
    
    
    
    /**
     * String to byte array. Removes spaces from the string
     * 
     * @param stringField , an standard string
     * @return byte array
     */

    public void write(){
    	int writtenBytes =  alreadyWritten[currentblock]+recordSize;
    	//Current block to write can never be more than 667
    	currentblock = createHash(title);
    	System.out.println("Written bytes in block :" + writtenBytes);
		if((1023 - recordSize - alreadyWritten[currentblock]) >= 3){
			try {
				block = buffer.acquireBlock(fc, currentblock);
				block.position(alreadyWritten[currentblock]);
			} catch (IOException e) {
				e.printStackTrace();
			}
			block.put(titleLength);    	
			block.put(title);
			block.put(nationalityLength);
			block.put(nationality);
			block.put(voLength);
			block.put(vo);
			block.put(year);
			
			for(int i = 0; i<16 ; i++){
				if(topicExist[i] == 1){
					block.put(topicExist[i]);
					block.put(topic[i]);
				}
				if(topicExist[i] == 0){
					block.put(topicExist[i]);
				}
			}
			block.put(length);
			block.put(takings);
			block.put(directorNameLength);
			block.put(directorName);
			block.put(directorSurnameLength);
			block.put(directorSurname);
			block.put(directorNicknameLength);
			block.put(directorNickname);
			block.put(screenwriterNameLength);
			block.put(screenwriterName);
			block.put(screenwriterSurnameLength);
			block.put(screenwriterSurname);
			block.put(screenwriterNicknameLength);
			block.put(screenwriterNickname);
			
			for(int i = 0; i<8; i++){
				if(actorExist[i] == 0){
					block.put(actorExist[i]);
				}
				if(actorExist[i] == 1){
					block.put(actorExist[i]);
					block.put(actorNameLength[i]);
					block.put(actorName[i]);
					block.put(actorSurnameLength[i]);
					block.put(actorSurname[i]);
					block.put(actorNicknameLength[i]);
					block.put(actorNickname[i]);
					}
				}
			
			alreadyWritten[currentblock] += recordSize;
			//Write PPL bytes
			block.position(1021);
			block.put(intToByteArray(alreadyWritten[currentblock]));
		}
		else{
			try {
				block = buffer.acquireBlock(fc, currentblock);
				//Write Overflow byte
				block.position(1023);
				block.put((byte) 1);
			} catch (IOException e) {
				e.printStackTrace();
			}
			String getValue;
			try {
				block = buffer.acquireBlock(fc, currentBlockInOverflowArea);
				block.clear();
			} catch (IOException e) {
				e.printStackTrace();
			}
			for(int i = 0 ; i<1024; i++){
				getValue = Byte.toString(block.get());
				if(getValue == null){
					break;
				}
			}
			alreadyWrittenOverflow = block.position();
			//Write serially. If 1024 bytes are reached, go for the next block
			writeOverflow(titleLength);
			writeOverflow(title);
			writeOverflow(nationalityLength);
			writeOverflow(nationality);
			writeOverflow(voLength);
			writeOverflow(vo);
			writeOverflow(year);
			for(int i = 0; i<16 ; i++){
				if(topicExist[i] == 1){
					writeOverflow(topicExist[i]);
					writeOverflow(topic[i]);
				}
				if(topicExist[i] == 0){
					writeOverflow(topicExist[i]);
				}
			}
			writeOverflow(length);
			writeOverflow(takings);
			writeOverflow(directorNameLength);
			writeOverflow(directorName);
			writeOverflow(directorSurnameLength);
			writeOverflow(directorSurname);
			writeOverflow(directorNicknameLength);
			writeOverflow(directorNickname);
			writeOverflow(screenwriterNameLength);
			writeOverflow(screenwriterName);
			writeOverflow(screenwriterSurnameLength);
			writeOverflow(screenwriterSurname);
			writeOverflow(screenwriterNicknameLength);
			writeOverflow(screenwriterNickname);
			for(int i = 0; i<8; i++){
				if(actorExist[i] == 0){
					writeOverflow(actorExist[i]);
				}
				if(actorExist[i] == 1){
					writeOverflow(actorExist[i]);
					writeOverflow(actorNameLength[i]);
					writeOverflow(actorName[i]);
					writeOverflow(actorSurnameLength[i]);
					writeOverflow(actorSurname[i]);
					writeOverflow(actorNicknameLength[i]);
					writeOverflow(actorNickname[i]);
				}
			}
		}
    }
    
    /**
     * Writes in the overflow area an array of bytes
     * 
     * @parameter an array of bytes
     * @return void
     */
    
    public void writeOverflow(byte[] myByteArray){
    	if((1024 - alreadyWrittenOverflow) > myByteArray.length){
			block.put(myByteArray);
			alreadyWrittenOverflow += myByteArray.length;
		}
		else{
			currentBlockInOverflowArea++;
			try {
				block = buffer.acquireBlock(fc, currentBlockInOverflowArea);
				block.clear();
				alreadyWrittenOverflow = 0;
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
    }
    
    public void writeOverflow(byte myByte){
    	if((1024 - alreadyWrittenOverflow) > 1){
			block.put(myByte);
			alreadyWrittenOverflow += 1;
		}
		else{
			currentBlockInOverflowArea++;
			try {
				block = buffer.acquireBlock(fc, currentBlockInOverflowArea);
				block.clear();
				alreadyWrittenOverflow = 0;
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
    }
    /**
     * Write the byte array to the opened file
     * 
     * @param stringField , an standard string
     * @return byte array
     */

    public byte[] stringToByte (String stringField){
    	byte byteArray[];
    	stringField = stringField.trim();
    	byteArray = new byte[stringField.length()];
    	byteArray = stringField.getBytes();
		return byteArray;
    }
    
    /**
     * Short to byte array
     * 
     * 
     */

    public static byte[] shortToByte(short s) {
        return new byte[]{(byte)(s & 0x00FF),(byte)((s & 0xFF00)>>8)};
    }
    /**
     * Get an array of 2 bytes out of an int. It is only used to get the PPL
     * 
     * @param integer to use
     */
    
    public static byte[] intToByteArray(int a){
		BigInteger bigInt = BigInteger.valueOf(a);
		return bigInt.toByteArray();
	}
    
    /**
     * readShort
     * 
     * 
     */
    
    public static short readShort(byte[] data, int offset) {
		return (short) (((data[offset] << 8)) | ((data[offset + 1] & 0xff)));
	}
    
    /**
     * Get an int out of a byte array
     * 
     * @param integer to use
     */
    
	public static int byteArrayToInt(byte[] bytes){
		int value = 0;
		for(int i=0; i<bytes.length;i++){
			value = (value << 8) + (bytes[i] & 0xff);
		}
		return value;
	}

    /**
    * Busca los registros de un fileName concreto que cumplen unas determinadas condiciones especificadas en inputRecord y devuelve el primero de ellos en outputRecord.
    * 
    * @param inputRecord registro l�gico recogido que contiene el estado de los registros en la interfaz de usuario en el momento de solicitar la consulta.
    * @param outputRecord registro l�gico en el que se devuelve el primer resultado de la b�squeda.
    * @return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */
	
    public String select(LogicalRecord inputRecord, LogicalRecord outputRecord) {
    	
        //Pone los valores del registro de salida a null
        for(String fieldName:inputRecord.getFieldNames()){
            outputRecord.setField(fieldName, inputRecord.getField(fieldName));
        }
        
        String inputTitle = inputRecord.getField("Title");
        int key;
        int titleLengthRead;
        int recordsCounter = 0;
        byte[] titleRead;
        boolean recordFound = false;
        
        if(inputTitle != null){
        	ByteBuffer readBuffer = null;
        	key = createHash(stringToByte(inputTitle));
        	try {
				readBuffer = buffer.acquireBlock(fc, key);
				readBuffer.clear();
			} catch (IOException e) {
				e.printStackTrace();
			}
			//Bring title.length
			while(!recordFound){
				titleLengthRead = readBuffer.get();
				titleRead = new byte[titleLengthRead];
				for(int i = 0 ; i<titleLengthRead; i++){
					titleRead[i] = readBuffer.get();
				}
				if(inputTitle.equals(new String(titleRead))){
					recordFound = true;
					outputRecord.setField("Title", inputTitle);
					setOutputRecords("Nationality", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("O.L.", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Year", 2, readBuffer, outputRecord);
					//Set topics. If topic.exist is set to 0, the field is set to ""
					for(int i = 0 ; i < 16; i++){
						if(readBuffer.get() == 0){
							//outputRecord.setField("Topic", "");
						}
						else{
							setOutputRecords("Topic", 1, readBuffer, outputRecord);
						}
					}
					setOutputRecords("Length (min)", 2, readBuffer, outputRecord);
					setOutputRecords("Takings (€)", 4, readBuffer, outputRecord);
					setOutputRecords("Director.Name", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Director.S1", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Director.S2", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Screen.Name", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Screen.S1", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Screen.Nickname", readBuffer.get(), readBuffer, outputRecord);
					//Set actors in the same way topics were set
					for(int i = 0 ; i < 8; i++){
						if(readBuffer.get() == 0){
							outputRecord.setField("Actor"+i+".Name", "");
							outputRecord.setField("Actor"+i+".S1", "");
							outputRecord.setField("Actor"+i+".Nickname", "");
						}
						else{
							setOutputRecords("Actor"+i+".Name", readBuffer.get(), readBuffer, outputRecord);
							setOutputRecords("Actor"+i+".S1", readBuffer.get(), readBuffer, outputRecord);
							setOutputRecords("Actor"+i+".Nickname", readBuffer.get(), readBuffer, outputRecord);
						}
					}
				}else{
					if(recordsCounter == 0){
						readRecord(readBuffer);
						recordsCounter++;
					}
					if(recordsCounter > 3){
						//Read the overflow area
						try {
							readBuffer = buffer.acquireBlock(fc, 693);
							readBuffer.clear();
						} catch (IOException e) {
							e.printStackTrace();
						}
						//Search serially
						boolean recordInOverflow = false;
						while(!recordInOverflow){
							recordInOverflow = searchRecord(readBuffer, inputTitle, outputRecord);
						}
					}
					if(recordsCounter > 1 && recordsCounter < 3){
						readRecordTitle(readBuffer);
						recordsCounter++;
					}
				}
			}
        	
        }
        return "Method 'FileManager.select' not implemented.";
    }
    
    /**
     * Search
     * 
     */
    
    public boolean searchRecord(ByteBuffer readBuffer, String inputTitle, LogicalRecord outputRecord){
    	int titleLengthRead = readBuffer.get();
		byte[] titleRead = new byte[titleLengthRead];
		boolean recordFound = false;
		for(int i = 0 ; i<titleLengthRead; i++){
			titleRead[i] = readBuffer.get();
		}
		if(inputTitle.equals(new String(titleRead))){
			recordFound = true;
			outputRecord.setField("Title", inputTitle);
			setOutputRecords("Nationality", readBuffer.get(), readBuffer, outputRecord);
			setOutputRecords("O.L.", readBuffer.get(), readBuffer, outputRecord);
			setOutputRecords("Year", 2, readBuffer, outputRecord);
			//Set topics. If topic.exist is set to 0, the field is set to ""
			for(int i = 0 ; i < 16; i++){
				if(readBuffer.get() == 0){
					//outputRecord.setField("Topic", "");
				}
				else{
					setOutputRecords("Topic", 1, readBuffer, outputRecord);
				}
			}
			setOutputRecords("Length (min)", 2, readBuffer, outputRecord);
			setOutputRecords("Takings (€)", 4, readBuffer, outputRecord);
			setOutputRecords("Director.Name", readBuffer.get(), readBuffer, outputRecord);
			setOutputRecords("Director.S1", readBuffer.get(), readBuffer, outputRecord);
			setOutputRecords("Director.S2", readBuffer.get(), readBuffer, outputRecord);
			setOutputRecords("Screen.Name", readBuffer.get(), readBuffer, outputRecord);
			setOutputRecords("Screen.S1", readBuffer.get(), readBuffer, outputRecord);
			setOutputRecords("Screen.Nickname", readBuffer.get(), readBuffer, outputRecord);
			//Set actors in the same way topics were set
			for(int i = 0 ; i < 8; i++){
				if(readBuffer.get() == 0){
					outputRecord.setField("Actor"+i+".Name", "");
					outputRecord.setField("Actor"+i+".S1", "");
					outputRecord.setField("Actor"+i+".Nickname", "");
				}
				else{
					setOutputRecords("Actor"+i+".Name", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Actor"+i+".S1", readBuffer.get(), readBuffer, outputRecord);
					setOutputRecords("Actor"+i+".Nickname", readBuffer.get(), readBuffer, outputRecord);
				}
			}
			return recordFound;
		}
		return recordFound;
    }
    
    /**
     * Read field
     * 
     * @param buffer
     */
    
    	public void readField(ByteBuffer readBuffer, int length){
    		if(length == 0){
    			int lengthOfField = readBuffer.get();
    			for(int i = 0; i<lengthOfField; i++){
    				readBuffer.get();
    			}
    		}
    		else{
    			for(int i = 0; i<length; i++){
    				readBuffer.get();
    			}
    		}
    	}
    
    /**
     * Read record
     * 
     * @param buffer
     */
    
    	public void readRecord(ByteBuffer readBuffer){
    		readField(readBuffer,0);
    		readField(readBuffer,0);
    		readField(readBuffer,0);
			readField(readBuffer,2);
			//Set topics. If topic.exist is set to 0, the field is set to ""
			for(int i = 0 ; i < 16; i++){
				if(readBuffer.get() == 0){
				}
				else{
					readField(readBuffer,0);
				}
			}
			readField(readBuffer, 2);
			readField(readBuffer, 4);
			readField(readBuffer, 0);
			readField(readBuffer, 0);
			readField(readBuffer, 0);
			readField(readBuffer, 0);
			readField(readBuffer, 0);
			readField(readBuffer, 0);
			//Set actors in the same way topics were set
			for(int i = 0 ; i < 8; i++){
				if(readBuffer.get() == 0){
				}
				else{
					readField(readBuffer, 0);
					readField(readBuffer, 0);
					readField(readBuffer, 0);
				}
			}
    	}
    
    	 /**
         * Read record but do not read the title
         * 
         * @param buffer
         */
        
        	public void readRecordTitle(ByteBuffer readBuffer){
        		readField(readBuffer,0);
        		readField(readBuffer,0);
    			readField(readBuffer,2);
    			//Set topics. If topic.exist is set to 0, the field is set to ""
    			for(int i = 0 ; i < 16; i++){
    				if(readBuffer.get() == 0){
    				}
    				else{
    					readField(readBuffer,0);
    				}
    			}
    			readField(readBuffer, 2);
    			readField(readBuffer, 4);
    			readField(readBuffer, 0);
    			readField(readBuffer, 0);
    			readField(readBuffer, 0);
    			readField(readBuffer, 0);
    			readField(readBuffer, 0);
    			readField(readBuffer, 0);
    			//Set actors in the same way topics were set
    			for(int i = 0 ; i < 8; i++){
    				if(readBuffer.get() == 0){
    				}
    				else{
    					readField(readBuffer, 0);
    					readField(readBuffer, 0);
    					readField(readBuffer, 0);
    				}
    			}
        	}
        
        
    /**
     * Set output records
     * 
     * @param string field
     * @param int length field
     * @param bytebuffer readbuffer
     * @return 
     */
    
   public void setOutputRecords (String field, int lengthField, ByteBuffer readBuffer, LogicalRecord outputRecord){
	   byte[] myArray = new byte [lengthField];
	   for(int i=0;i<lengthField;i++){
		   myArray[i] = readBuffer.get();
	   }
	   if(field.contains("Takings (€)")){
		   outputRecord.setField(field,Integer.toString(byteArrayToInt(myArray)));
	   }
	   else if(field.contains("Year")){
	      outputRecord.setField(field,Integer.toString(readShort(myArray,0)));
	   }
	   else{
		   outputRecord.setField(field,new String(myArray));
	   }
	   
   }
   
   /**
    * Recupera el siguiente registro que cumple el criterio de b�squeda actual
    * 
    * @param outputRecord registro l�gico en el que se devuelve el registro siguiente.
    * @return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */
    public String nextRecord(LogicalRecord outputRecord) {
    	// Get field names
    	 String[] FieldNames = outputRecord.getFieldNames();
    	// Once field names are known load blocks on buffer until this fields are found
    	// if this fields are found return the next record
         return "Method 'FileManager.nextRecord(LogicalRecord outputRecord)' not implemented";
    }

   /**
    * Recupera el registro anterior que cumple con el criterio de b�squeda actual. 
    * 
    * @param outputRecord registro l�gico en el que se devuelve el registro anterior.
    * @return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */
    public String previousRecord(LogicalRecord outputRecord) {
        return "Method 'FileManager.previousRecord(LogicalRecord outputRecord)' not implemented";
    }    

     /**
    * M�todo para realizar una b�squeda por acceso invertido.  
    *
    * @param inputRecord Registro l�gico que contiene el estado de los registros en la interfaz de usuario en el momento de solicitar la consulta.
    * @param results Vector de Strings en el que se guardar�n los resultados de la invocaci�n del m�todo. Deber�n concatenarse en cada una de estas cadenas de caracteres todos los campos requeridos en el acceso invertido.
    * @return El n�mero de resultados listados.
    */  
    
    public int invertedAccess(LogicalRecord inputRecord, Vector<String> results) {
        results.add("Method 'FileManager.invertedAccess(LogicalRecord inputRecord, Vector<String> results)' not implemented");
        return results.size();
    }
    
    /*Ejecuci�n del sistema gestor de ficheros con la interfaz gr�fica de usuario.*/
    public static void main(String arg[]){
        /* Se lanza una nueva ventana principal con una nueva instancia de 
        GestorDeFicheros y una nueva instancia de Esquema*/
        UserInterface.launch(new FileManager());
    } 
}
