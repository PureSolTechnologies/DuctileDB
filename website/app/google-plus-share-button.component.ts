import {Component} from 'angular2/core';

@Component({
  selector: 'google-plus-share-button',
  template: `<div class="g-plus" data-action="share" data-annotation="bubble"></div>`
})
export class GooglePlusShareButtonComponent {
  ngAfterContentInit() {
    gapi.plus.go();
  }
}
