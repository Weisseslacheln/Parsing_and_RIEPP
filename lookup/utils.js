export function create_normal_fio(fio) {
  //нижний регистр и замена букв ё,й на е,и соответственно
  return fio.toLowerCase().replace(/ё/g, "е").replace(/й/g, "и");
}
