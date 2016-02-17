import {Component} from 'angular2/core';

declare var gapi: any;

@Component({
  selector: 'google-plus-share-button',
  template: `<div class="g-plus" data-action="share" data-annotation="bubble"></div>`
})
export class GooglePlusShareButtonComponent {
  ngAfterContentInit() {
    if (typeof gapi !== 'undefined' && gapi != null) {
      gapi.plus.go();
    }
  }
}
