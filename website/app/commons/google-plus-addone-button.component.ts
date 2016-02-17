import {Component} from 'angular2/core';

declare var gapi: any;

@Component({
  selector: 'google-plus-addone-button',
  template: `<div class="g-plusone"></div>`  
})
export class GooglePlusAddoneButtonComponent {
  ngAfterContentInit() {
    if (typeof gapi !== 'undefined' && gapi != null) {
      gapi.plusone.go();
    }
  }
}
