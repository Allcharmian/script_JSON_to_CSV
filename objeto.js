
function main(){
    objeto_nuev = new Object();
    objeto_nuev.salud = 200;
    objeto_nuev.atack = 10;

    let numero = 5;
    console.log('el numero inicial es:', numero);

    numero = cambio(numero);
    console.log('el nuevo numero es: ', numero)


    console.log(objeto_nuev);
}

main()



function cambio(numero){
    numero += 2;
    return numero
}
