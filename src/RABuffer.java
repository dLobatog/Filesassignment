import fileSystem.utils.*;
import java.nio.channels.FileChannel;

/**
 *Implementación de una memoria intermedia con política de liberación aleatoria
 */

public class RABuffer extends Buffer{
    
        /**
         *Constructor de una memoria intermedia con política de liberación 
         *aleatoria de 16 páginas de 1024 bytes cada una.
         */
        public RABuffer(){
            super();
        }
        
        /**
         * Constructor de una memoria intermedia con política de liberación 
         * aleatoria con un número de páginas y tamaño de bloque a definir.
         * 
         * @param numberOfpages Número de páginas de la memoria intermedia
         * @param blockSize Tamaño de bloque de las páginas de la memoria 
         * intermedia.
         */
        public RABuffer(int numberOfpages, int blockSize){
            super(numberOfpages,blockSize);
        }
        
        /**
         *Método devuelve el identificador de la página a liberar durante 
         *la carga de un bloque de memoria, cuando no quedan páginas libres, 
         *siguiendo una política aleatoria.
         *@param fc El descriptor del fichero al que pertenece el bloque que se desea cargar.
         *@param blockNumber El número del bloque que se desea cargar del fichero identificado por fc.
         *@return el identificador de la página en la que se escribirá el bloque.
         */
        public int releasePagePolicy(FileChannel fc, int blockNumber) {
                int pagina =(int)Math.floor(Math.random()*this.getNumberOfPages());
                System.out.println("\tFull memory. Page "+pagina+" is released to load block "+blockNumber+" (file "+fc+").");
		return pagina;
        }
        
        /**
         *Método que permite controlar cuándo una página es utilizada. 
         *Cada vez que se utiliza una determinada página de la memoria 
         *intermedia se invoca este método.
         *@param i Página que ha sido utilizada.
         */
        public void referencedPage(int i) {
            System.out.println("\tReferenced page "+i);
            //Esta memoria no realiza ninguna acción cuando se utiliza alguna página.
        }
}
