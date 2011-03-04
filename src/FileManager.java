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
    	ByteBuffer bb;
    	boolean EOF = false;
    	int eofCounter = 0;
    	int currentBlock = 0;
    	int counter = 0;
    	int usefulCounter = 0;
    	int realCounter = 0;
        //Keep track of how many bytes are left in block
        int bytesRead=0;
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
				//Get title and convert it to the new physical-logical design
				bytesOfString = new byte[70];
				//block.get(bytesOfString) 
				for(int j = 0 ; j < 70 ; j++){
					bytesOfString[j] = block.get();
					bytesRead++;
					if(bytesOfString[j] == 35){
						eofCounter ++;
					}
					if(bytesRead == 1024){
							currentBlock++;
							counter++;
							bytesRead=0;
							buffer.close(importfc);
							importfc = buffer.openFile(fileName, "rw");
							block = buffer.acquireBlock(importfc, currentBlock);
							block.clear();
							System.out.println(currentBlock + " file channel position (import fc): " + importfc.position());
					}
				}
				stringField = new String(bytesOfString);
				title = stringToByte(stringField);
				titleLength = (byte) title.length;
				if (eofCounter >= 4){
					System.out.println("Useful bytes " + usefulCounter);
					System.out.println("Nr of records " + counter);
					System.out.println("EOF reached. Stopping..");
					EOF = true;
					/* only for debug purposes */ System.out.println("####");
				}
				else{
					counter++;
					usefulCounter = usefulCounter + title.length;
					realCounter = usefulCounter + 1;
					//Get nationality and convert it to the new physical logical design
					bytesOfString = new byte[14];
					for(int j = 0 ; j < 14 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
					//Get vo and convert it to the new physical-logical design
					bytesOfString = new byte[12];
					for(int j = 0 ; j < 12 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					year = stringToByte(stringField);
					/* only for debug purposes */ System.out.println(new String(year));
					usefulCounter = usefulCounter + year.length;
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
									buffer.close(importfc);
									importfc = buffer.openFile(fileName, "rw");
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						topic[k] = stringToByte(stringField);
						topicLength[k] = (byte) topic[k].length;
						usefulCounter = usefulCounter + topic[k].length;
						//This means that there exist a topic in position # k.
						if(topicLength[k]!=0){
							topicExist[k] = 1;
							realCounter = usefulCounter + 1;
							/* only for debug purposes */ System.out.println(new String(topic[k]));
						}
						else{
							topicExist[k]=0;
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
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					length = Short.parseShort(stringField);
					/* only for debug purposes */ System.out.println(length);
					usefulCounter = usefulCounter + 2;
					//Get takings and convert it to the new physical-logical design 
					bytesOfString = new byte[9];
					for(int j = 0 ; j < 9 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
								block = buffer.acquireBlock(importfc, currentBlock);
								block.clear();
								System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
						}
					}
					stringField = new String(bytesOfString);
					takings = Integer.parseInt(stringField);
					/* only for debug purposes */ System.out.println(takings);
					usefulCounter = usefulCounter + 4;
					//Get director's name and convert it to the new physical-logical design
					bytesOfString = new byte[35];
					for(int j = 0 ; j < 35 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
					//Get director's surname and convert it to the new physical-logical design
					bytesOfString = new byte[15];
					for(int j = 0 ; j < 15 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
					//Get director's nickname and convert it to the new physical-logical design
					bytesOfString = new byte[25];
					for(int j = 0 ; j < 25 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
					//Get screenwriters's name and convert it to the new physical-logical design
					bytesOfString = new byte[35];
					for(int j = 0 ; j < 35 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
					//Get screenwriter's surname and convert it to the new physical-logical design
					bytesOfString = new byte[15];
					for(int j = 0 ; j < 15 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
					//Get screenwriter's nickname and convert it to the new physical-logical design
					bytesOfString = new byte[25];
					for(int j = 0 ; j < 25 ; j++){
						bytesOfString[j] = block.get();
						bytesRead++;
						if(bytesRead == 1024){
								currentBlock++;
								counter++;
								bytesRead=0;
								buffer.close(importfc);
								importfc = buffer.openFile(fileName, "rw");
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
									buffer.close(importfc);
									importfc = buffer.openFile(fileName, "rw");
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						actorName[k] = stringToByte(stringField);
						actorNameLength[k] = (byte) actorName[k].length;
						usefulCounter = usefulCounter + actorName[k].length;
						//Get actor's surname and convert it to the new physical-logical design
						bytesOfString = new byte[15];
						for(int j = 0 ; j < 15 ; j++){
							bytesOfString[j] = block.get();
							bytesRead++;
							if(bytesRead == 1024){
									currentBlock++;
									counter++;
									bytesRead=0;
									buffer.close(importfc);
									importfc = buffer.openFile(fileName, "rw");
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						actorSurname[k] = stringToByte(stringField);
						actorSurnameLength[k] = (byte) actorSurname[k].length;
						usefulCounter = usefulCounter + actorSurname[k].length;
						//Get actor's nickname and convert it to the new physical-logical design
						bytesOfString = new byte[25];
						for(int j = 0 ; j < 25 ; j++){
							bytesOfString[j] = block.get();
							bytesRead++;
							if(bytesRead == 1024){
									currentBlock++;
									counter++;
									bytesRead=0;
									buffer.close(importfc);
									importfc = buffer.openFile(fileName, "rw");
									block = buffer.acquireBlock(importfc, currentBlock);
									block.clear();
									System.out.println(currentBlock  + " file channel position (import fc): " + importfc.position());
							}
						}
						stringField = new String(bytesOfString);
						actorNickname[k] = stringToByte(stringField);
						actorNicknameLength[k] = (byte) actorNickname[k].length;
						usefulCounter = usefulCounter + actorNickname[k].length;
						//This means that there exist a topic in position # k.
						if(actorNicknameLength[k]!=0){
							actorExist[k] = 1;
							/* only for debug purposes */ System.out.println(new String(actorName[k]));
							/* only for debug purposes */ System.out.println(new String(actorSurname[k]));
							/* only for debug purposes */ System.out.println(new String(actorNickname[k]));
						}
						else{
							actorExist[k]=0;
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
    		// Add conditions to change it to the new design
    	}
    	
        return "Method 'FileManager.importFile("+fileName+")' not implemented";
    }
    
    /**
     * String to byte array. Removes spaces from the string
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
     * Left bytes in block. Returns how many bytes are left in the current block
     * 
     * @param stringField , an standard string
     * @return byte array
     */

    public int leftBytesInBlock (){
    	
    	return 1;
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
