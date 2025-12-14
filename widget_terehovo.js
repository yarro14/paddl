(function (w, d, s, o, f, js, fjs) {
    w[o] = w[o] || function () { (w[o].q = w[o].q || []).push(arguments) };
    let isWidgetReady = false;

    let loader = d.createElement('div');
    loader.className = 'vc-widget-loader-container';
    loader.innerHTML = '<div class="vc-widget-loader"></div>';
    
    let scriptTag = d.currentScript;
    scriptTag.parentNode.insertBefore(loader, scriptTag);

    const style = d.createElement('style');
    style.innerHTML = `
      :root {
        --accent-color: #4734F8;
      }
      
      @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
      }
      
      .vc-widget-loader {
        display: inline-block;
        width: 40px;
        height: 40px;
        animation: spin 1.4s linear infinite;
        position: relative;
      }

      .vc-widget-loader::before {
        content: '';
        position: absolute;
        inset: 0;
        border-radius: 50px;
        padding: 4px;
        background: linear-gradient(45deg, var(--accent-color, #4734F8), #ffffff);
        -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
        -webkit-mask-composite: xor;
        mask-composite: exclude;
      }

      .vc-widget-loader-container {
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 10px;
      }
    `;
    d.head.appendChild(style);

    js = d.createElement(s), fjs = document.currentScript || d.getElementsByTagName(s)[0];
    js.id = o; js.src = f; js.async = 1;
    
    js.onload = function () {
      isWidgetReady = true;
      loader.remove();
    };

    fjs.parentNode.insertBefore(js, fjs);
})(window, document, 'script', 'PadlTerekhovo', 'https://cabinet.vivacrm.ru/widget.js');

// Специальная логика для виджетов с переопределением staticWidgetMode

// Изолируем область видимости для каждого виджета
(function() {
  // Читаем параметры из script тега 
  const currentScript = document.currentScript || document.querySelector('script[src*="817e679b-20c5-4832-84a2-0fc3b15634db.js"]');
  let overrideStaticMode = undefined;

  if (currentScript) {
    // Проверяем атрибут staticwidgetmode
    const staticModeAttr = currentScript.getAttribute('staticwidgetmode');
    if (staticModeAttr !== null) {
      overrideStaticMode = staticModeAttr === 'true';
    }
    
    // Проверяем URL параметры
    const scriptSrc = currentScript.src;
    if (scriptSrc) {
      const url = new URL(scriptSrc);
      const staticModeParam = url.searchParams.get('staticwidgetmode');
      if (staticModeParam !== null) {
        overrideStaticMode = staticModeParam === 'true';
      }
    }
  }

  const originalConfig = {
  "theme": "light",
  "blocks": [],
  "studio": "станция",
  "channel": "whatsapp",
  "currency": "RUB",
  "darkLogo": "",
  "language": "ru",
  "lightLogo": "",
  "tenantKey": "iSkq6G",
  "vocabulary": {
    "room": "корт",
    "class": "занятие",
    "place": "локация",
    "action": "записаться",
    "master": "исполнитель",
    "service": "тренировка"
  },
  "bookingDays": 14,
  "description": "",
  "borderRadius": true,
  "displaySteps": true,
  "userContacts": {
    "requestLastName": true,
    "lastNameRequired": true,
    "requestFirstName": true,
    "firstNameRequired": true
  },
  "widgetStyles": {
    "fontFamily": "Onest",
    "accentColor": "#4734F8",
    "secondColor": "#4734F8",
    "tertiaryColor": "#333333",
    "backgroundColor": "#ffffff"
  },
  "hideselectors": false,
  "masterServiceId": "2f4155ad-7bc0-4a15-a12c-da7fce15c37a",
  "publicOfferLink": "https://padlhub.ru/docs",
  "staticWidgetMode": true,
  "displayCostOfSlot": true,
  "timeBeforeBooking": [
    {
      "id": "1",
      "time": 0,
      "timeWithoutTrainer": 0
    }
  ],
  "phoneInputSettings": {
    "code": "+7",
    "country": "ru"
  },
  "hideSelectedOptions": false,
  "preventIsolatedSlots": false,
  "slotPriceWithoutGradeImpact": false,
  "personalDataProcessingPolicyLink": "https://ffc.team/politica"
};
  const finalConfig = {...originalConfig};

  // Применяем переопределение staticWidgetMode если есть
  if (overrideStaticMode !== undefined) {
    finalConfig.staticWidgetMode = overrideStaticMode;
  }

  window['PadlTerekhovo']('init', finalConfig);
})();

// Добавляем popup-collection поддержку только для групповых виджетов


