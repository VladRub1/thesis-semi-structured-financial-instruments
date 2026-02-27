/**
 * Literals
 */
const showLessTitle = 'Показать меньше';
const showMoreTitle = 'Показать больше';
const showAllFiles = 'Показать все файлы';
const hideOldRevisionModeClass = "displayNone";
const INACTIVE_REDACTION_TOGGLE_SELECTOR = ".inactive-redaction-toggle";
const INACTIVE_ORDER_INFO_SELECTOR = '.inactive-order-info';

$(document).ready(function () {
    const querySelector = document.querySelector('.switch');
    $('.switch .inactive-redaction-toggle:not(.active-switch)').prop('checked', false);

    if (querySelector !== null && querySelector) {
        querySelector.addEventListener('click', toggleActiveRevisionView);

        //проверяем для всех дивов attachmentViewToggle есть ли у данного документа скрытые атачменты - если нет, то "Показать все файлы" не отображается
        // Иначе - проводим инициализацию события, по клику на div отображаем скрытое содержимое
        const attachment = ".attachment";
        $("#attachmentViewToggle").each(function () {
            const td = $(this).closest("td");
            if (td.find(".nonperformingedition").size() == 0) {
                $(this).hide();
            } else {
                if (td.find().size() > 0) {
                    td.find(attachment).hide();
                    $(this).children(".empty").html(showAllFiles);
                }
                $(this).onSwitch(
                    function () {
                        td.find(attachment).show();
                        $(this).children(".empty").html("Скрыть недействительные файлы");
                    },
                    function () {
                        td.find(attachment).hide();
                        $(this).children(".empty").html(showAllFiles);
                    }
                );
            }
        });
    }

    $('.controlArrow').on('click', function () {
        const $this = $(this).find(".controlArrowSpan");
        const cr = $('.controlResult' + $this.data("id"));

        cr.toggleClass('hidden');
        $this.toggleClass('collapceArrow');

        if ($this.hasClass('collapceArrow')) {
            $this.siblings('span').html(showLessTitle);
        } else {
            $this.siblings('span').html(showMoreTitle);
        }
    });

});

function openClosedFilesDocs(element) {
    $(element).siblings('.closedFilesDocs').toggleClass(hideOldRevisionModeClass);
    changeAttachmentTitle(element);
}

function toggleHiddenAttachment(toggleAttachmentEl) {
    $(toggleAttachmentEl).parent().find(".hidden-attachment").toggleClass("d-none");
    changeAttachmentTitle(toggleAttachmentEl);
}

function changeAttachmentTitle(toggleAttachmentEl){
    if ($(toggleAttachmentEl).text() === showMoreTitle) {
        $(toggleAttachmentEl).text(showLessTitle);
    } else {
        $(toggleAttachmentEl).text(showMoreTitle);
    }
}


function toggleActiveRevisionView(event) {
    if (event.target.type === 'checkbox') {
        return false;
    }
    const hideOldRevisionMode = $(INACTIVE_REDACTION_TOGGLE_SELECTOR).prop('checked');
    if (hideOldRevisionMode) {
        $(INACTIVE_REDACTION_TOGGLE_SELECTOR).attr('checked', false);
        $('.notice-documents').find(INACTIVE_ORDER_INFO_SELECTOR).addClass(hideOldRevisionModeClass);
        $('.notice-documents.inactive-order-info').addClass(hideOldRevisionModeClass);
        $('.first-row-active-documents').addClass("closedInactiveDocuments");
        $('.notice-documents .hr').parent(INACTIVE_ORDER_INFO_SELECTOR).addClass(hideOldRevisionModeClass);
        $(".controlInfosBlock.inactive-order-info").addClass(hideOldRevisionModeClass);
        if (document.querySelector('.container .classSelectorAttach.inactiveElement')) {
            $('.card-edition-container .card-edition-inactive').addClass(hideOldRevisionModeClass);
        }
        $('.edition.no-valid').addClass('d-none');
        $('.edition.valid').removeClass('d-none');
        $('.attachment-show-switcher-handler .inactive-order-info').addClass(hideOldRevisionModeClass);
    } else {
        $(INACTIVE_REDACTION_TOGGLE_SELECTOR).attr('checked', true);
        $('.notice-documents').find(INACTIVE_ORDER_INFO_SELECTOR).removeClass(hideOldRevisionModeClass);
        $('.notice-documents.inactive-order-info').removeClass(hideOldRevisionModeClass);
        $('.first-row-active-documents').removeClass("closedInactiveDocuments");
        $('.notice-documents .hr').parent(INACTIVE_ORDER_INFO_SELECTOR).removeClass(hideOldRevisionModeClass);
        $(".controlInfosBlock.inactive-order-info").removeClass(hideOldRevisionModeClass);
        if (document.querySelector('.container .classSelectorAttach.inactiveElement')) {
            $('.card-edition-container .card-edition-inactive').removeClass(hideOldRevisionModeClass);
        }
        $('.edition.no-valid').removeClass('d-none');
        $('.edition.valid').addClass('d-none');
        $('.attachment-show-switcher-handler .inactive-order-info').removeClass(hideOldRevisionModeClass);
    }

}
