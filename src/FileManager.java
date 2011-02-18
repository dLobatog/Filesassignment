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
    byte titleLength, nationalityLength, voLength, topicLength, topicExist,
    nameLength, surnameLength, nicknameLength, nicknameExist;
    // Arrays where the fields will be saved according to the physical logical design. 
    byte [] title = new byte[titleLength]; 
    byte [] nationality = new byte[nationalityLength];
    byte [] vo = new byte[voLength];
    byte year;
    { // Define variables if they exist
    	if (topicExist == 1){
    		// Topic may be designed as a linked list or as a bidimensional array
    		byte [] topic = new byte[topicLength];
    	}
    }
    byte length;
    byte takings;
    {	
    	if(nicknameExist==1){
    		byte [][][] director = new byte [nameLength][surnameLength][nicknameLength];
    	}
    	else{
    		byte [][] director = new byte [nameLength][surnameLength];
    	}
    }
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
    	try {
			fc = buffer.openFile(fileName, "rw");
			block=buffer.acquireBlock(fc,0);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		
		//Determine whether the file contains any data or not
		if(block != null){
			System.out.println("File is not empty.");
			// Define data file???
			// Position the pointer at the beggining of the block
			block.clear();
			// Print buffer status
			buffer.print();
		}
		else{
			System.out.println("File is empty.");
			// Define data file
			//Position the pointer at the beggining of the block
			block.clear();
			// Print buffer status
			buffer.print();
		}

        return "Method 'FileManager.openFileSystem("+fileName+")' not implemented";
    }

    /**
    *Cierra el sistema de ficheros. Previamente vuelca los datos de la memoria intermedia y cierra los ficheros abiertos. 
    *@return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */
    public String closeFileSystem() {      
    	//Set the policy to release all the pages in the block to intermediate memory.
    	buffer.releasePagePolicy(fc, buffer.getNumberOfPages());
    	buffer.close(fc);
        return "Method 'FileManager.closeFileSystem()' not implemented";
    }

    /**
    * Fuerza la escritura del contenido de la memoria intermedia en los ficheros correspondientes.  
    *@return Devuelve una cadena de caracteres que se mostrar� en la parte inferior de la ventana de interfaz como resultado de la ejecuci�n de este m�todo.
    */  
    public String flush() {
    	//Save the buffer in the file
    	buffer.save(fc);
        return "Method 'FileManager.flush()' not implemented";
    }

    /**
    * Lee un fichero de organizaci�n serial consecutiva y dise�o inicial de los registros y almacena su contenido 
    * en los nuevos ficheros dise�ados.
    * 
    * @param fileName Nombre completo del archivo desde el que se importa
    */
    public String importFile(String fileName) {
    	//Loop until Title = #### is found
		//Read Registry
		//Change to new design
		//Save
    	//Close old file
    	for(int i=0;i<buffer.getNumberOfPages();i++){
    		try {
				buffer.acquireBlock(fc, i);
			} catch (IOException e) {
				e.printStackTrace();
			}
    		// Add conditions to change it to the new design
    	}
    	
        return "Method 'FileManager.importFile("+fileName+")' not implemented";
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
