function detectGeolocation(callback) {
    navigator.geolocation.getCurrentPosition(function (position) {
            $.ajax({
                url: "https://nominatim.openstreetmap.org/reverse",
                data: {
                    lat: position.coords.latitude,
                    lon: position.coords.longitude,
                    format: "json",
                    zoom: "10"
                },
                dataType: "json",
                type: "GET",
                crossDomain: true
            }).then(function (result) {
                callback(result);
            });
        },
        function (error) {
            // если юзер в IOS не разрешил определение геолокации - не запрашивать геолокацию 1 день
            $.cookie("doNotAdviseToChangeLocationWhenIosReject", "true", {path: '/epz', expires: 1});
            console.log(error.message);
        }
    )
}

function getRegionIdByName(regionName, callback) {
    $.get('/epz/api/nsi/subjectRf/regionIdByName.json', {regionName: regionName}).done(function (data) {
        callback(data);
    });
}

function getAdministrativeAreaName(responseFromOMSGeolocationService) {
    return responseFromOMSGeolocationService.address.state;
}

function getSubAdministrativeAreaName(responseFromOMSGeolocationService) {
    return responseFromOMSGeolocationService.address.city;
}

function setRegionNameHeader(regionName) {
    $("#popUpUserRegion").text(regionName);
}

function prepareRegionName(regionName) {
    if (regionName === 'Москва и Московская область') {
        return 'Московская';
    }

    if (regionName === 'Москва и область') {
        return 'Московская';
    }

    if (regionName === 'Санкт-Петербург и Ленинградская область') {
        return 'Ленинградская';
    }

    if (regionName === 'Санкт-Петербург и область') {
        return 'Ленинградская';
    }

    let resultRegionName = null;
    const stringToRemoveArray = [' автономный округ', ' автономная область', ' АО', ' область', ' край', 'Республика ', ' Республика', ' республика'];

    for (let j = 0; j < stringToRemoveArray.length; j++) {
        if (regionName.search(stringToRemoveArray[j]) > -1) {
            resultRegionName = regionName.replace(stringToRemoveArray[j], '');

            break;
        } else {
            resultRegionName = regionName;
        }
    }

    return resultRegionName.split('-')[0];
}

function findFederalRegionAnswer(answer) {
    const answerArray = answer.split(' ');

    for (let i = 0; i < answerArray.length; i++) {
        if (answerArray[i] === 'округ') {
            return true;
        }
    }

    return false;
}


/*Отладочная информация яндекс мап (было до oms) - все регионы РФ из ответа Яндекса https://yandex.ru/yaca/geo.c2n */
/*var yandexRegionNameArray = ['Москва и область', 'Белгородская область', 'Брянская область', 'Владимирcкая область', 'Воронежcкая область', 'Ивановская область', 'Калужская область', 'Костромская область', 'Курская область',
 'Липецкая область', 'Орловская область', 'Рязанская область', 'Смоленская область', 'Тамбовская область', 'Тверская область', 'Тульская область', 'Ярославская область',
 'Санкт-Петербург и Ленинградская область', 'Архангельская область', 'Вологодская область', 'Калининградская область', 'Мурманская область',
 'Новгородская область', 'Псковская область', 'Республика Коми', 'Республика Карелия', 'Ненецкий АО',
 'Астраханская область', 'Волгоградская область', 'Краснодарский край', 'Республика Адыгея', 'Республика Калмыкия', 'Ростовская область',
 'Кировская область', 'Республика Марий Эл', 'Нижегородская область', 'Оренбургская область', 'Пензенская область', 'Пермский край', 'Республика Башкортостан',
 'Республика Мордовия', 'Татарстан', 'Самарская область', 'Саратовская область', 'Удмуртская республика', 'Ульяновская область', 'Чувашская республика',
 'Курганская область', 'Свердловская область', 'Тюменская область', 'Ханты-Мансийский АО', 'Челябинская область', 'Ямало-Ненецкий АО',
 'Алтайский край', 'Иркутская область', 'Кемеровская область', 'Красноярский край', 'Новосибирская область', 'Омская область',
 'Республика Алтай', 'Республика Бурятия', 'Республика Тыва', 'Республика Хакасия', 'Томская область', 'Забайкальский край',
 'Магаданская область', 'Камчатский край', 'Еврейская автономная область', 'Чукотский автономный округ', 'Хабаровский край', 'Приморский край', 'Амурская область', 'Республика Саха (Якутия)', 'Сахалинская область',
 'Республика Дагестан', 'Республика Ингушетия', 'Республика Кабардино-Балкария', 'Республика Северная Осетия-Алания', 'Ставропольский край', 'Карачаево-Черкесская Республика', 'Чеченская Республика', 'Крым', 'Севастополь'];*/