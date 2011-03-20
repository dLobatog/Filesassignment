import fileSystem.utils.AbstractFileManager;
import fileSystem.utils.LogicalRecord;
import fileSystem.utils.Buffer;
import fileSystem.utils.UserInterface;

import java.io.FileNotFoundException;
import java.io.IOException;
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
    ByteBuffer blockToWrite = null;
    int currentBlockToWrite = 0;
    int[] alreadyWritten = new int[692];
    int alreadyWrittenOverflow = 0;
    int hashArray[] = new int[692];
    int hashCounter = 0;
    // Length and existence marks
    byte titleLength, nationalityLength, voLength,
    nameLength, surnameLength, nicknameLength, nicknameExist;
    // Arrays where the fields will be saved according to the physical logical design.
    byte [] topicExist = new byte[16];
    byte [] topicLength = new byte[16];
    byte [] title = new byte[titleLength]; 
    byte [] nationality = new byte[nationalityLength];
    byte [] vo = new byte[voLength];
    byte [] year;
    // Topic may be designed as a linked list or as a bidimensional array
    byte [][] topic = new byte[16][];
    // A short in java is 2 bytes so length could be of this type
    short length;
    // An int in java is 4 bytes so takings could be of this type
    int takings;
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
    	//Open file fileName with all permissions allowed and get first block
    	boolean haveData=false;
    	try {
			fc = buffer.openFile(fileName, "rw");
			block=buffer.acquireBlock(fc,0);
			if (block != null){
				haveData = true;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		
		if (haveData){
			return "File system ("+fileName+")' is now open and it contains data";
		}
		else{
			return "File system ("+fileName+") is now open and it does not contain data";
		}
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
    	Buffer importBuffer;
    	importBuffer = new RABuffer();
    	boolean EOF = false;
    	int eofCounter = 0;
    	int currentBlock = 0;
    	int counter = 0;
    	int usefulCounter = 0;
    	int realCounter = 0;
    	int recordSize = 0;
        //Keep track of how many bytes are left in block
        int bytesRead = 0;
        int bytesWritten = 0;
        
		try {
			importfc = importBuffer.openFile(fileName, "rw");
			block = importBuffer.acquireBlock(importfc, currentBlock);
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
							counter++;
							bytesRead=0;
							block = importBuffer.acquireBlock(importfc, currentBlock);
							block.clear();
							System.out.println(currentBlock + " file channel position (import fc): " + importfc.position());
					}
				}
				stringField = new String(bytesOfString);
		    	title = stringToByte(stringField);
				titleLength = (byte) title.length;
				System.out.println(stringField);
				if (eofCounter >= 4){
					System.out.println("Useful bytes " + usefulCounter);
					System.out.println("AVG. useful bytes/record " + usefulCounter/counter);
					System.out.println("Real bytes "+ realCounter);
					System.out.println("AVG. reals bytes/record " + realCounter/counter);
					System.out.println("Nr of records " + counter);
					System.out.println("Repeated hash keys: "+ hashCounter);
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
					for(int j = 0				 ; j < 14 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					year = stringToByte(stringField);
					/* only for debug purposes */ System.out.println(new String(year));
					usefulCounter = usefulCounter + year.length;
					realCounter = realCounter + year.length;
					recordSize = recordSize + year.length;
					//Get the topics. In our design a mark of existence will be written before the set of fields.
					//This mark will consist in a byte specifying how many topics there will be
					bytesOfString = new byte[15];
					for(int k = 0; k < 16; k++){
						for(int j = 0 ; j < 15 ; j++){
							bytesOfString[j] = block.get();
							bytesRead++;
							if(bytesRead == 1024){
									currentBlock++;
									counter++;
									bytesRead=0;
									 
									 
									block = importBuffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						topic[k] = stringToByte(stringField);
						topicLength[k] = (byte) topic[k].length;
						usefulCounter = usefulCounter + topic[k].length;
						realCounter = realCounter + topic[k].length + 1;
						recordSize = recordSize + topic[k].length + 1;
						//This means that there exist a topic in position # k.
						if(topicLength[k]!=0){
							topicExist[k] = 1;
							realCounter = realCounter + 1;
							recordSize = recordSize + 1;
							/* only for debug purposes */ System.out.println(new String(topic[k]));
						}
						else{
							topicExist[k]=0;
							realCounter = realCounter + 1;
							recordSize = recordSize + 1;
						}
					}
					//Get length of the movie and convert it to the new physical-logical design
					bytesOfString = new byte[3];
					for(int j = 0 ; j < 3 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					/* only for debug purposes */
					length = Short.parseShort(stringField);
					System.out.println(length);
																				
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					takings = Integer.parseInt(stringField);
					/* only for debug purposes */ System.out.println(takings);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
								counter++;
								bytesRead=0;
								 
								 
								block = importBuffer.acquireBlock(importfc, currentBlock);
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
									counter++;
									bytesRead=0;
									 
									 
									block = importBuffer.acquireBlock(importfc, currentBlock);
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
									counter++;
									bytesRead=0;
									 
									 
									block = importBuffer.acquireBlock(importfc, currentBlock);
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
									counter++;
									bytesRead=0;
									 
									 
									block = importBuffer.acquireBlock(importfc, currentBlock);
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
				}
			// Write to file
			// TODO : Solve error
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
    	
    	for (int i = 0 ; i <(title.length/2) ; i ++){
			titleSum1 += title[i];
		}
		for (int i = (title.length/2) ; i <title.length ; i ++){
			titleSum2 += title[i];
		}
		
		key = (titleSum1 + titleSum2) % 692;
    	return key;
    }
    

    /**
     * String to byte array. Removes spaces from the string
     * 
     * @param stringField , an standard string
     * @return byte array
     */

    public void write(){
    	int totalBytes = getTotalBytes();
    	//Current block to write can never be more than 692
    	currentBlockToWrite = createHash(title);
    	
		if((1024 - totalBytes - alreadyWritten[currentBlockToWrite]) >= 3){
			try {
				blockToWrite = buffer.acquireBlock(fc, currentBlockToWrite);
				blockToWrite.position(alreadyWritten[currentBlockToWrite]);
			} catch (IOException e) {
				e.printStackTrace();
			}
			blockToWrite.put(titleLength);    	
			blockToWrite.put(title);
			blockToWrite.put(nationality);
			blockToWrite.put(nationalityLength);
			blockToWrite.put(vo);
			blockToWrite.put(voLength);
			blockToWrite.put(year);
			for(int i = 0; i<16 ; i++){
				if(topicExist[i] == 1){
					blockToWrite.put(topicLength[i]);
					blockToWrite.put(topicExist[i]);
				}
				if(topicExist[i] == 0){
					blockToWrite.put(topicExist[i]);
				}
			}
			blockToWrite.put((byte) length);
			blockToWrite.put((byte) takings);
			blockToWrite.put(directorName);
			blockToWrite.put(directorNameLength);
			blockToWrite.put(directorSurname);
			blockToWrite.put(directorSurnameLength);
			blockToWrite.put(directorNickname);
			blockToWrite.put(directorNicknameLength);
			blockToWrite.put(screenwriterName);
			blockToWrite.put(screenwriterNameLength);
			blockToWrite.put(screenwriterSurname);
			blockToWrite.put(screenwriterSurnameLength);
			blockToWrite.put(screenwriterNickname);
			blockToWrite.put(screenwriterNicknameLength);
			for(int i = 0; i<8; i++){
				if(actorExist[i] == 0){
					blockToWrite.put(actorExist[i]);
				}
				if(actorExist[i] == 1){
					blockToWrite.put(actorExist[i]);
					blockToWrite.put(actorName[i]);
					blockToWrite.put(actorNameLength[i]);
					blockToWrite.put(actorSurname[i]);
					blockToWrite.put(actorSurnameLength[i]);
					blockToWrite.put(actorNickname[i]);
					blockToWrite.put(actorNicknameLength[i]);
				}
			}
			alreadyWritten[currentBlockToWrite] += totalBytes;
		}
		else{
			int currentBlockInOverflowArea = 693;
			String getValue;
			try {
				blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
				blockToWrite.clear();
			} catch (IOException e) {
				e.printStackTrace();
			}
			for(int i = 0 ; i<1024; i++){
				getValue = Byte.toString(blockToWrite.get());
				if(getValue == null){
					break;
				}
			}
			alreadyWrittenOverflow = blockToWrite.position();
			//Write serially. If 1024 bytes are reached, go for the next block
			if((1024 - alreadyWrittenOverflow) > 1){
				blockToWrite.put(titleLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > title.length){
				blockToWrite.put(title);
				alreadyWrittenOverflow += title.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 -alreadyWrittenOverflow) > 1){
				blockToWrite.put(nationalityLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > nationality.length){	
				blockToWrite.put(nationality);
				alreadyWrittenOverflow += nationality.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 -alreadyWrittenOverflow) > 1){
				blockToWrite.put(voLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > vo.length){
				blockToWrite.put(vo);
				alreadyWrittenOverflow += vo.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > year.length){
				blockToWrite.put(year);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			for(int i = 0; i<16 ; i++){
				if(topicExist[i] == 1){
					if((1024 - alreadyWrittenOverflow) > 1){
						blockToWrite.put(topicLength[i]);
						alreadyWrittenOverflow += 1;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
					if((1024 - alreadyWrittenOverflow) > topic[i].length){
						blockToWrite.put(topic[i]);
						alreadyWrittenOverflow += topic[i].length;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
				}
				if((1024 - alreadyWrittenOverflow) > 1){
					blockToWrite.put(topicLength[i]);
					alreadyWrittenOverflow += 1;
				}
				else{
					currentBlockInOverflowArea++;
					try {
						blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
						blockToWrite.clear();
						alreadyWrittenOverflow = 0;
					} catch (IOException e) {
						e.printStackTrace();
					}	
				}
			}
			if((1024 - alreadyWrittenOverflow) > 2){
				blockToWrite.put((byte) length);
				alreadyWrittenOverflow += 2;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > 4){
				blockToWrite.put((byte) takings);
				alreadyWrittenOverflow += 4;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > 1){
				blockToWrite.put(directorNameLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > directorName.length){
				blockToWrite.put(directorName);
				alreadyWrittenOverflow += directorName.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > 1){
				blockToWrite.put(directorSurnameLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > directorSurname.length){
				blockToWrite.put(directorSurname);
				alreadyWrittenOverflow += directorSurname.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > 1 ){
				blockToWrite.put(directorNicknameLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try{
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > directorNickname.length){
				blockToWrite.put(directorNickname);
				alreadyWrittenOverflow += directorNickname.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > 1 ){
				blockToWrite.put(screenwriterNameLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > screenwriterName.length){
				blockToWrite.put(screenwriterName);
				alreadyWrittenOverflow += screenwriterName.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > 1 ){
				blockToWrite.put(screenwriterSurnameLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > screenwriterSurname.length){
				blockToWrite.put(screenwriterSurname);
				alreadyWrittenOverflow += screenwriterSurname.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > 1 ){
				blockToWrite.put(screenwriterNicknameLength);
				alreadyWrittenOverflow += 1;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			if((1024 - alreadyWrittenOverflow) > screenwriterNickname.length){
				blockToWrite.put(screenwriterNickname);
				alreadyWrittenOverflow += screenwriterNickname.length;
			}
			else{
				currentBlockInOverflowArea++;
				try {
					blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
					blockToWrite.clear();
					alreadyWrittenOverflow = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
			for(int i = 0; i<8; i++){
				if(actorExist[i] == 0){
					if((1024 - alreadyWrittenOverflow) > 1 ){
						blockToWrite.put(actorExist[i]);
						alreadyWrittenOverflow += 1;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
				}
				if(actorExist[i] == 1){
					if((1024 - alreadyWrittenOverflow) > 1 ){
						blockToWrite.put(actorExist[i]);
						alreadyWrittenOverflow += 1;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
					if((1024 - alreadyWrittenOverflow) > 1){
						blockToWrite.put(actorNameLength[i]);
						alreadyWrittenOverflow += 1;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
					if((1024 - alreadyWrittenOverflow) > actorName[i].length ){
						blockToWrite.put(actorName[i]);
						alreadyWrittenOverflow += actorName[i].length;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
					if((1024 - alreadyWrittenOverflow) > 1){
						blockToWrite.put(actorSurnameLength[i]);
						alreadyWrittenOverflow += 1;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
					if((1024 - alreadyWrittenOverflow) > actorSurname[i].length){
						blockToWrite.put(actorSurname[i]);
						alreadyWrittenOverflow += actorSurname[i].length;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
					if((1024 - alreadyWrittenOverflow) > 1){
						blockToWrite.put(actorNicknameLength[i]);
						alreadyWrittenOverflow += 1;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
					if((1024 - alreadyWrittenOverflow) > actorNickname[i].length){
						blockToWrite.put(actorNickname[i]);
						alreadyWrittenOverflow += actorNickname.length;
					}
					else{
						currentBlockInOverflowArea++;
						try {
							blockToWrite = buffer.acquireBlock(fc, currentBlockInOverflowArea);
							blockToWrite.clear();
							alreadyWrittenOverflow = 0;
						} catch (IOException e) {
							e.printStackTrace();
						}	
					}
				}
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
     * Get total bytes
     * 
     * @return int
     */

    public int getTotalBytes (){
    	int totalTopics = 0;
    	int totalActors = 0;
    	int totalTopicLength = 0;
    	int totalActorLength = 0;
    	
    	for(int i = 0 ; i<16; i++){
    		if(topicExist[i] == 1){
    			totalTopics++;
    			totalTopicLength += topicLength[i]; 
    		}
    	}
    	for(int i = 0 ; i<7; i++){
    		if(actorExist[i] == 1){
    			totalActors++;
    			totalActorLength += actorNameLength[i];
    			totalActorLength += actorSurnameLength[i];
    			totalActorLength += actorNicknameLength[i];
    		}
    	}
    	
    	return title.length + 1 + nationality.length + 1 + vo.length + 1 + year.length + 2 + 4 
    	+ directorName.length + 1 +directorSurname.length + 1 + directorNickname.length + 1 +
    	+ screenwriterName.length + 1 + screenwriterSurname.length + 1 + screenwriterNickname.length + 1 +
    	+ directorName.length + 1 +directorSurname.length + 1 + directorNickname.length + 1 + totalActors
    	+ totalTopics + totalTopicLength + 3*totalTopics  + totalActors + totalActorLength + 3*totalActors;
		
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
            outputRecord.setField(fieldName,"null");
        }
        
        //Se crea una cadena de caracteres a partir del registro de entrada
        String inputString=new String();
        for(String fieldName:inputRecord.getFieldNames()){
            inputString=inputString+inputRecord.getField(fieldName)+", ";
        }
        if(inputString.length()>10)inputString=inputString.subSequence(0,10)+"...";
        
        //Se crea otra a partir del de salida
        String outputString=new String();
        for(String fieldName:outputRecord.getFieldNames()){
            outputString=outputString+outputRecord.getField(fieldName)+", ";
        }
        if(outputString.length()>10)outputString=outputString.subSequence(0,10)+"...";
        return "Method 'FileManager.select(<"+inputString+">, <"+outputString+">)' not implemented.";
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
