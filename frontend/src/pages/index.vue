<script lang="ts" setup>
import DropperComponent from '@/components/DropperComponent.vue';
import SuperDuperAnimationComponent from '@/components/SuperDuperAnimationComponent.vue';


</script>

<template>
  <header class="header">
    <div class="logo">
      <img src="@/assets/logo_narfu.png" alt="Логотип">
    </div>

    <nav class="nav-menu">
      <a href="#about">Возможности</a>
      <a href="#services">Услуги</a>
      <a href="#contact">Контакты</a>
    </nav>



    <div class="logo">
      <img src="@/assets/512px-Roscosmos_logo_ru.svg.png" alt="Логотип">
    </div>
    <div class="burger-menu">
      ☰
    </div>
  </header>

  <!-- Мобильное меню -->
  <nav class="mobile-menu">
    <a href="#about">Возможности</a>
    <a href="#services">Услуги</a>
    <a href="#contact">Контакты</a>
  </nav>
  <!-- Блок 2 Заголовок-->
  <section class="parallax">
    <h1>Сервис по автоматизированному определению кромки льда </h1>
    <h2>по данным радиолокационной съемки и природно-климатическим данным</h2>

  </section>

  <!-- Блок 3 Карточки преимуществ-->
  <section class="advantages" id=about>
    <div class="cards-container">
      <div class="card">
        <img src="@/assets/cove.png" alt="Преимущество">
        <h3>Граница льда</h3>
        <p>Сервис автоматически определяет границу льда по данным радиолокационной съемки и природно-климатическим
          данным</p>

      </div>
      <div class="card">
        <img src="@/assets/measuring-tool.png" alt="Преимущество">
        <h3>Метрика IoU не менее 0,85 </h3>
        <p>Сервис обеспечивает метрику IoU (Intersection over Union) не менее 0,85 для определения кромки льда.</p>

      </div>
      <div class="card">
        <img src="@/assets/map.png" alt="Преимущество">
        <h3>Карта расположения кромки льда</h3>
        <p>Сервис автоматически строит карту расположения кромки льда на основе обработанных данных</p>

      </div>
    </div>
  </section>
  <!-- Блок 4 Параллакс с картой-->
  <section class="parallax1">
    <h1>Сервис автоматически строит карту расположения кромки льда на основе обработанных данных</h1>

  </section>
  <!-- Блок 5 Возможности-->
  <section class="content-section">
    <div class="content-image content-image1"></div>
    <div class="content-text">
      <h2>Безопасное судоходство</h2>
      <p>Ледовая обстановка динамична и может меняться очень быстро. В процессе судоходства как по морю, так и в устьях
        рек важным является информация о границе чистой воды и кромки льда, что влияет на движение судов низкого
        ледового класса в осенний-весенний период.</p>
      <p>Сервис позволяет в автоматизированном режиме строить карту расположения кромки льда по данным радиолокационной
        съемки и природно-климатическим данным точностью IoU более 0,85.</p>
      <p>Данное решение позволяет знать границу безопасного судоходства в море и устье рек близ береговой линии и на
        основании полученной карты строить маршрут плавания. </p>
    </div>

  </section>
  <!-- Блок 6 Применение -->
  <section class="content-section">
    <div class="content-text">
      <h2>Востребованность информации, предоставляемой сервисом</h2>
      <p>Подобная информация может быть востребована УГМС для предоставления её органам государственной власти, отраслей
        экономики, органов МЧС, населения о сложившихся гидрологических условиях.</p>
      <p>Сервис поможет при проведении поисково-спасательных операций в ледовых условиях, информировании судовладельцев
        о ледовой обстановке, взаимодействии с другими организациями, занимающимися мониторингом и прогнозированием
        ледовой обстановки, а также с ледокольными компаниями. </p>
    </div>
    <div class="content-image content-image2"></div>
  </section>


  <!-- Блок 7 Варианты-->
  <section class="advantages" id=services>
    <h2>Варианты предоставления данных</h2>
    <div class="cards-container">
      <div class="card">
        <img src="@/assets/snimok1.png" alt="Преимущество">

        <p class="services-p">Радиолокационный снимок, с распознанной границей льда</p>

      </div>
      <div class="card">
        <img src="@/assets/snimok2.png" alt="Преимущество">

        <p class="services-p">Истинная маска границ льда</p>

      </div>
    </div>
  </section>

  <section class="advantages">
    <h2>Протестируйте</h2>
    <DropperComponent />
  </section>

  <!-- Блок 8 Призыв -->
  <section class="cta-section">
    <a href="https://forms.yandex.ru/u/67e0580f49363982216d5ddc/" target="_blank"> <button class="cta-btn">Начать
        сотрудничество</button></a>
  </section>

  <!-- Блок 9 Контакты -->
  <section class="text-section">
    <h2 id=contact>Контакты</h2>
    <p>Высшая школа информационных технологий и автоматизированных систем,</p>
    <p> 163002, г. Архангельск, наб. Северной Двины, 2, корпус 2, </p>
    <p>+7 (8182) 21-61-00 (доб. 19-23) </p>
  </section>

  <!-- Блок 10 подвал -->
  <footer>
    <nav class="footer-menu">
      <a href="#about">Возможности</a>
      <a href="#services">Услуги</a>
      <a href="#contact">Контакты</a>
    </nav>
    <p>&copy; 2025 Высшая школа информационных технологий и автоматизированных систем. </p>
    <p>Сервис по автоматизированному определению кромки льда. Все права защищены</p>
  </footer>
</template>

<style lang="scss">
:root {
  --primary-blue: #147e8f;
  --secondary-blue: #4e9ab6;
  --accent-blue: #97bac9;
  --text-dark: #2d3436;
  --text-light: #ffffff;
  --bg-light: #f8fafb;
  --radius: 15px;
  --shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
  --gradient-blue: linear-gradient(135deg, #147e8f 0%, #4e9ab6 100%);
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
  font-family: 'Arial', sans-serif;
  scroll-behavior: smooth;
}

body {
  display: flex;
  flex-direction: column;
}

/* Блок 1: Фиксированная шапка */
.header {
  position: fixed;
  top: 0;
  width: 100%;
  padding: 1rem;
  background: var(--text-light);
  display: flex;
  justify-content: space-between;
  align-items: center;
  z-index: 1000;
  box-shadow: var(--shadow);
}

.logo img {
  height: 50px;
}

.nav-menu {
  display: flex;
  gap: 2rem;
}

.nav-menu a {
  color: var(--text-dark);
  text-decoration: none;
  font-weight: 600;
  transition: color 0.3s;
}

.nav-menu a:hover {
  color: var(--primary-blue);
  font-weight: 600;
}

.burger-menu {
  display: none;
  cursor: pointer;
}

/* Блок 2, 4: Параллакс-секция */
.parallax {
  height: 100vh;
  background:
    linear-gradient(rgba(255, 255, 255, 0.1), rgba(0, 0, 0, 0.3)),
    url('@/assets/photo_5395662531319689206_w.jpg') fixed center/cover;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  color: var(--text-light);
  padding-top: 80px;
  flex-direction: column;
}

.parallax1 {
  height: 40vh;
  background:
    linear-gradient(rgba(255, 255, 255, 0.1), rgba(0, 0, 0, 0.3)),
    url('@/assets/snimok3.png') fixed center/cover;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  color: var(--text-light);
  padding-top: 60px;
  flex-direction: column;
}

.parallax h1 {
  font-size: 4rem;
  text-shadow: 3px 3px 10px rgba(0, 0, 0, 0.5);
}

.parallax h2 {
  font-size: 3rem;
  text-shadow: 3px 3px 8px rgba(0, 0, 0, 0.5);
}

/* Блок 3,7: Карточки преимуществ */
.advantages {
  padding: 4rem 2rem;
  background: var(--bg-light);
  display: flex;
  flex-direction: column;
  align-items: center;
  align-content: center;
}

.cards-container {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 2rem;
  max-width: 1200px;
  margin: 0 auto;
}

.card {
  background: var(--text-light);
  border-radius: var(--radius);
  padding: 2rem;
  position: relative;
  overflow: hidden;
  transition: transform 0.3s;
  box-shadow: var(--shadow);

}

.card:hover {
  transform: translateY(-10px);

}

.card::after {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'%3E%3Crect x='2' y='2' width='96' height='96' fill='none' stroke='%23147e8f' stroke-width='4' stroke-dasharray='400' stroke-dashoffset='400'/%3E%3C/svg%3E");
  animation: drawBorder 1.2s ease-out forwards;
}

.card:hover::after {
  opacity: 1;

}

@keyframes drawBorder {
  to {
    stroke-dashoffset: 0;
  }
}

.card img {
  width: 100%;
  height: 200px;
  object-fit: contain;
  border-radius: var(--radius);
  margin-bottom: 1rem;
}

.card p {
  margin-left: 10px;

}

.card h3 {
  margin: 0 0 10px 10px;

}

.card-btn {
  background: var(--gradient-blue);
  color: var(--text-light);
  padding: 0.8rem 2rem;
  border: none;
  border-radius: var(--radius);
  cursor: pointer;
  margin-top: 1rem;
  transition: transform 0.9s;
  display: block;
  width: fit-content;
  margin: 1.5rem auto 0;
}

.card-btn:hover {
  transform: scale(1.05);
}

/* Блок 5,6: Текст + Изображение */
.content-section {
  display: flex;
  align-items: center;
  padding: 4rem 2rem;
  gap: 3rem;
  max-width: 1200px;
  margin: 0 auto;
}

.services-p {
  text-align: center;
}

.content-text {
  flex: 1;
  padding: 2rem;
  position: relative;
  padding-right: 60px;
}

.content-text::after {
  content: '';
  position: absolute;
  top: -20px;
  right: 0px;
  width: 400px;
  height: 120%;

  opacity: 0.6;
  animation: flow 20s linear infinite;
}




.content-text h2 {
  margin: 10px 0;
}

.content-text p {
  margin: 15px 0;
}

.content-image {
  flex: 1;
  height: 350px;
  /* background: url('coastline.jpg') center/cover;*/
  border-radius: var(--radius);
  box-shadow: var(--shadow);
}


.content-image1 {

  background: url('@/assets/photo_5395662531319689203_w.jpg') center/cover;

}

.content-image2 {

  background: url('@/assets/photo_5395662531319689213_w.jpg') center/cover;

}

/* Блок 8: CTA */
.cta-section {
  padding: 4rem 2rem;
  text-align: center;
  background: var(--gradient-blue);
  color: var(--text-light);
}

.cta-btn {
  background: var(--text-light);
  color: var(--primary-blue);
  padding: 1.2rem 3rem;
  font-size: 1.2rem;
  border: none;
  border-radius: var(--radius);
  cursor: pointer;
  transition: all 0.3s;
  box-shadow: var(--shadow);
}

.cta-btn:hover {
  transform: translateY(-3px);
}

.advantages {
  color: black;

  & h2 {
    padding: 3rem 2rem;
    background: var(--bg-light);
    text-align: center;
  }
}

/* Блок 9: Текстовый блок */
.text-section {
  padding: 4rem 2rem;
  background: var(--bg-light);
  text-align: center;
}

.text-section p {
  margin: 10px 0;
}

/* Блок 10: Футер */
footer {
  background: var(--text-dark);
  color: var(--text-light);
  padding: 3rem 2rem;
  text-align: center;
}

.footer-menu {
  display: flex;
  justify-content: center;
  gap: 2rem;
  margin-bottom: 2rem;
}

.footer-menu a {
  color: var(--text-light);
  text-decoration: none;
}

/* Мобильное меню */
.mobile-menu {
  display: none;
  position: fixed;
  top: 70px;
  right: 0;
  background: var(--text-light);
  width: 100%;
  padding: 1rem;
  box-shadow: var(--shadow);
  z-index: 999;
}

.mobile-menu.active {
  display: block;
}

.mobile-menu a {
  display: block;
  padding: 1rem;
  text-align: center;
  color: var(--text-dark);
  text-decoration: none;
}

/* Адаптивность */
@media (max-width: 38rem) {
  .nav-menu {
    display: none;
  }

  .burger-menu {
    display: block;
  }

  .parallax h1 {
    font-size: 2rem;
    padding: 0 3em;
  }

  .parallax h2 {
    font-size: 1.5rem;
    padding: 0 3em;
  }

  .content-section {
    flex-direction: column;
  }


  .cards-container {
    grid-template-columns: 1fr;

  }

  .footer-menu {
    flex-direction: column;
    gap: 1rem;
  }

  .content-text::after {
    width: 120px;
    right: -30px;
    top: -15%;
    transform: rotate(10deg);
  }
}
</style>
