import {Component} from 'angular2/core';

@Component({
  selector: 'google-plus-addone-button',
  template: `<div class="g-plusone"></div>`  
})
export class GooglePlusAddoneButtonComponent {
  ngAfterContentInit() {
    gapi.plusone.go();
  }
}
